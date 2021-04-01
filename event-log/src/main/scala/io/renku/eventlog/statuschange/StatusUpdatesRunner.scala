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

import cats.data.Kleisli
import cats.effect.{Async, Bracket, IO}
import cats.free.Free
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.statuschange.IOUpdateCommandsRunner.QueryFailedException
import io.renku.eventlog.statuschange.commands.UpdateResult.{NotFound, Updated}
import io.renku.eventlog.statuschange.commands.{ChangeStatusCommand, UpdateResult}
import skunk._
import skunk.implicits._
import skunk.codec.all._
import skunk.data.Completion

import scala.util.control.NonFatal

trait StatusUpdatesRunner[Interpretation[_]] {
  def run(command: ChangeStatusCommand[Interpretation]): Interpretation[UpdateResult]
}

class StatusUpdatesRunnerImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    transactor:       SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name],
    logger:           Logger[Interpretation]
) extends DbClient(Some(queriesExecTimes))
    with StatusUpdatesRunner[Interpretation]
    with StatusProcessingTime {

  override def run(command: ChangeStatusCommand[Interpretation]): Interpretation[UpdateResult] = transactor.use {
    implicit session =>
      session.transaction.use { transaction =>
        for {
          sp           <- transaction.savepoint
          updateResult <- executeCommand(command) recoverWith errorToUpdateResult(command, transaction.rollback(sp))
          _            <- logInfo(command, updateResult)
          _            <- command updateGauges updateResult
        } yield updateResult
      }
  }

  private def errorToUpdateResult(command:  ChangeStatusCommand[Interpretation],
                                  rollback: => Interpretation[Completion]
  ): PartialFunction[Throwable, Interpretation[UpdateResult]] = {
    case QueryFailedException(failedQueryName: SqlQuery.Name, updateResult: UpdateResult) =>
      rollback.flatMap { _ =>
        logger
          .info(
            s"$failedQueryName failed for event ${command.eventId} to status ${command.status} with result $updateResult. " +
              s"Rolling back queries: ${command.queries.map(_.name.toString).toList.mkString(", ")}"
          )
          .map(_ => updateResult)
      }

    case NonFatal(exception) =>
      rollback.map { _ =>
        UpdateResult
          .Failure(
            Refined.unsafeApply(
              s"${command.queries.map(_.name.toString).toList.mkString(", ")} failed for event ${command.eventId} to status ${command.status} with ${exception.getMessage}. " +
                s"Rolling back queries: ${command.queries.map(_.name.toString).toList.mkString(", ")}"
            )
          )
      }
  }

  private def logInfo(command: ChangeStatusCommand[Interpretation], updateResult: UpdateResult) = updateResult match {
    case Updated => logger.info(s"Event ${command.eventId} got ${command.status}")
    case _       => ().pure[Interpretation]
  }

  private def executeCommand(
      command:        ChangeStatusCommand[Interpretation]
  )(implicit session: Session[Interpretation]): Interpretation[UpdateResult] =
    checkIfPersisted(command.eventId) >>= {
      case true =>
        for {
          _      <- deleteDelivery(command) recoverWith logError(command)
          result <- runUpdateQueriesIfSuccessful(command) >>= toUpdateResult(command)
        } yield result
      case false => NotFound.pure[Interpretation].widen[commands.UpdateResult]
    }

  private def deleteDelivery(command: ChangeStatusCommand[Interpretation])(implicit session: Session[Interpretation]) =
    measureExecutionTime {
      SqlQuery(
        Kleisli { session =>
          val query: Command[EventId ~ projects.Id] = sql"""DELETE FROM event_delivery 
                            WHERE event_id = $eventIdPut AND project_id = $projectIdPut
                         """.command
          session.prepare(query).use(_.execute(command.eventId.id ~ command.eventId.projectId).void)
        },
        name = "status update - delivery info remove"
      )
    }

  private def logError(
      command: ChangeStatusCommand[Interpretation]
  ): PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)(s"Event ${command.eventId} could not be updated in eventDelivery")
  }

  private def toUpdateResult(
      command: ChangeStatusCommand[Interpretation]
  ): ((Int, SqlQuery[Interpretation, Int])) => Interpretation[UpdateResult] = { case (intResult, lastQuery) =>
    command.mapResult(intResult) match {
      case result if result != Updated =>
        QueryFailedException(lastQuery.name, result).raiseError[Interpretation, UpdateResult]
      case result => result.pure[Interpretation]
    }
  }

  private def checkIfPersisted(eventId: CompoundEventId)(implicit session: Session[Interpretation]) =
    measureExecutionTime {
      SqlQuery(
        Kleisli { session =>
          val query: Query[EventId ~ projects.Id, EventId] =
            sql"""SELECT event_id
                       FROM event
                       WHERE event_id = $eventIdPut AND project_id = $projectIdPut"""
              .query(eventIdGet)
          session.prepare(query).use(_.option(eventId.id ~ eventId.projectId)).map(_.isDefined)
        },
        name = "event update check existence"
      )
    }

  private def runUpdateQueriesIfSuccessful(
      command:        ChangeStatusCommand[Interpretation]
  )(implicit session: Session[Interpretation]): Interpretation[(Int, SqlQuery[Interpretation, Int])] =
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
            (previousResult, previousQuery, List.empty[SqlQuery[Interpretation, Int]]).pure[Interpretation]
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
