/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.eventlog.statuschange

import cats.effect.{Bracket, IO}
import cats.free.Free
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.metrics.LabeledHistogram
import doobie.free.connection
import doobie.free.connection.ConnectionIO
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.statuschange.IOUpdateCommandsRunner.QueryFailedException
import io.renku.eventlog.statuschange.commands.UpdateResult.{NotFound, Updated}
import io.renku.eventlog.statuschange.commands.{ChangeStatusCommand, UpdateResult}

import scala.util.control.NonFatal

trait StatusUpdatesRunner[Interpretation[_]] {
  def run(command: ChangeStatusCommand[Interpretation]): Interpretation[UpdateResult]
}

class StatusUpdatesRunnerImpl(
    transactor:       SessionResource[IO, EventLogDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
    logger:           Logger[IO]
)(implicit ME:        Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with StatusUpdatesRunner[IO]
    with StatusProcessingTime {

  private implicit val transact: SessionResource[IO, EventLogDB] = transactor

  import doobie.implicits._

  override def run(command: ChangeStatusCommand[IO]): IO[UpdateResult] =
    for {
      updateResult <- executeCommand(command) transact transactor.resource recoverWith errorToUpdateResult(command)
      _            <- logInfo(command, updateResult)
      _            <- command updateGauges updateResult
    } yield updateResult

  private def errorToUpdateResult(command: ChangeStatusCommand[IO]): PartialFunction[Throwable, IO[UpdateResult]] = {
    case QueryFailedException(failedQueryName: SqlQuery.Name, updateResult: UpdateResult) =>
      logger
        .info(
          s"$failedQueryName failed for event ${command.eventId} to status ${command.status} with result $updateResult. " +
            s"Rolling back queries: ${command.queries.map(_.name.toString).toList.mkString(", ")}"
        )
        .map(_ => updateResult)

    case NonFatal(exception) =>
      UpdateResult
        .Failure(
          Refined.unsafeApply(
            s"${command.queries.map(_.name.toString).toList.mkString(", ")} failed for event ${command.eventId} to status ${command.status} with ${exception.getMessage}. " +
              s"Rolling back queries: ${command.queries.map(_.name.toString).toList.mkString(", ")}"
          )
        )
        .pure[IO]
  }

  private def logInfo(command: ChangeStatusCommand[IO], updateResult: UpdateResult) = updateResult match {
    case Updated => logger.info(s"Event ${command.eventId} got ${command.status}")
    case _       => ME.unit
  }

  private def executeCommand(command: ChangeStatusCommand[IO]): Free[connection.ConnectionOp, UpdateResult] =
    checkIfPersisted(command.eventId) >>= {
      case true =>
        for {
          _      <- deleteDelivery(command) recoverWith logError(command)
          result <- runUpdateQueriesIfSuccessful(command) >>= toUpdateResult(command)
        } yield result
      case false => NotFound.pure[ConnectionIO].widen[commands.UpdateResult]
    }

  private def deleteDelivery(command: ChangeStatusCommand[IO]) = measureExecutionTime {
    SqlQuery(
      sql"""|DELETE FROM event_delivery 
            |WHERE event_id = ${command.eventId.id} AND project_id = ${command.eventId.projectId}
            |""".stripMargin.update.run.void,
      name = "status update - delivery info remove"
    )
  }

  private def logError(command: ChangeStatusCommand[IO]): PartialFunction[Throwable, ConnectionIO[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"Event ${command.eventId} could not be updated in eventDelivery").to[ConnectionIO]
  }

  private def toUpdateResult(command: ChangeStatusCommand[IO]): ((Int, SqlQuery[Int])) => ConnectionIO[UpdateResult] = {
    case (intResult, lastQuery) =>
      command.mapResult(intResult) match {
        case result if result != Updated =>
          QueryFailedException(lastQuery.name, result).raiseError[ConnectionIO, UpdateResult]
        case result => result.pure[ConnectionIO]
      }
  }

  private def checkIfPersisted(eventId: CompoundEventId) = measureExecutionTime {
    SqlQuery(
      sql"""|SELECT event_id
            |FROM event
            |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId}""".stripMargin
        .query[String]
        .option
        .map(_.isDefined),
      name = "event update check existence"
    )
  }

  private def runUpdateQueriesIfSuccessful(command: ChangeStatusCommand[IO]): ConnectionIO[(Int, SqlQuery[Int])] =
    measureExecutionTime(command.queries.head).flatMap { result =>
      val queries =
        upsertStatusProcessingTime(command.eventId, command.status, command.maybeProcessingTime)
          .map(processingTimeQuery => command.queries.tail :+ processingTimeQuery)
          .getOrElse(command.queries.tail)
      (result, command.queries.head, queries)
        .iterateWhileM {
          case (_, _, query :: remainingQueries) =>
            measureExecutionTime(query).map(result => (result, query, remainingQueries))
          case (previousResult, previousQuery, Nil) =>
            (previousResult, previousQuery, List.empty[SqlQuery[Int]]).pure[ConnectionIO]
        } { case (previsousResult, _, queries) => previsousResult == 1 && queries.nonEmpty }
        .map { case (lastResult, lastQuery, _) => (lastResult, lastQuery) }
    }

}

object IOUpdateCommandsRunner {

  import cats.effect.IO

  def apply(transactor:       SessionResource[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
            logger:           Logger[IO]
  ): IO[StatusUpdatesRunner[IO]] = IO {
    new StatusUpdatesRunnerImpl(transactor, queriesExecTimes, logger)
  }

  case class QueryFailedException(failingQueryName: SqlQuery.Name, updateResult: UpdateResult) extends Throwable()
}
