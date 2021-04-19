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
import ch.datascience.data.ErrorMessage
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.metrics.LabeledHistogram
import doobie.free.connection
import doobie.free.connection.ConnectionIO
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import org.typelevel.log4cats.Logger
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.statuschange.commands.UpdateResult.{NotFound, Updated}
import io.renku.eventlog.statuschange.commands.{ChangeStatusCommand, UpdateResult}

import scala.util.control.NonFatal

trait StatusUpdatesRunner[Interpretation[_]] {
  def run(command: ChangeStatusCommand[Interpretation]): Interpretation[UpdateResult]
}

class StatusUpdatesRunnerImpl(
    transactor:       DbTransactor[IO, EventLogDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
    logger:           Logger[IO]
)(implicit ME:        Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with StatusUpdatesRunner[IO]
    with StatusProcessingTime {

  private implicit val transact: DbTransactor[IO, EventLogDB] = transactor

  import doobie.implicits._

  override def run(command: ChangeStatusCommand[IO]): IO[UpdateResult] = for {
    updateResult <- (deleteDelivery(command) >> executeCommand(command))
                      .transact(transactor.get)
                      .recoverWith(deliveryCleanUp(command))
    _ <- logInfo(command, updateResult)
    _ <- command updateGauges updateResult
  } yield updateResult

  private def deliveryCleanUp(command: ChangeStatusCommand[IO]): PartialFunction[Throwable, IO[UpdateResult]] = {
    case NonFatal(exception) =>
      deleteDelivery(command)
        .transact(transactor.get)
        .recoverWith(deliveryErrorToResult(command)) >>= {
        case UpdateResult.Updated => logAndAsResult(s"Event ${command.eventId} got ${command.status}", exception)
        case deletionResult       => deletionResult.pure[IO]
      }
  }

  private def deliveryErrorToResult(command: ChangeStatusCommand[IO]): PartialFunction[Throwable, IO[UpdateResult]] = {
    case NonFatal(exception) =>
      logAndAsResult(s"Event ${command.eventId} - cannot remove event delivery", exception)
  }

  private def logAndAsResult(message: String, cause: Throwable): IO[UpdateResult] = {
    logger.error(cause)(message)
    UpdateResult.Failure(ErrorMessage.withExceptionMessage(cause)).pure[IO]
  }

  private def logInfo(command: ChangeStatusCommand[IO], updateResult: UpdateResult) = updateResult match {
    case Updated => logger.info(s"Event ${command.eventId} got ${command.status}")
    case _       => ME.unit
  }

  private def executeCommand(command: ChangeStatusCommand[IO]): Free[connection.ConnectionOp, UpdateResult] =
    checkIfPersisted(command.eventId) >>= {
      case true =>
        executeQueries(
          queries = command.queries.toList :++ maybeUpdateProcessingTimeQuery(command),
          command
        ) >>= toUpdateResult
      case false => NotFound.pure[ConnectionIO].widen[commands.UpdateResult]
    }

  private def deleteDelivery(command: ChangeStatusCommand[IO]) = measureExecutionTime {
    SqlQuery(
      sql"""|DELETE FROM event_delivery 
            |WHERE event_id = ${command.eventId.id} AND project_id = ${command.eventId.projectId}
            |""".stripMargin.update.run.map(_ => UpdateResult.Updated: UpdateResult),
      name = "status update - delivery info remove"
    )
  }

  private def toUpdateResult: UpdateResult => ConnectionIO[UpdateResult] = {
    case UpdateResult.Failure(message) => new Exception(message).raiseError[ConnectionIO, UpdateResult]
    case result                        => result.pure[ConnectionIO]
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

  private def maybeUpdateProcessingTimeQuery(command: ChangeStatusCommand[IO]) =
    upsertStatusProcessingTime(command.eventId, command.status, command.maybeProcessingTime)

  private def executeQueries(queries: List[SqlQuery[Int]],
                             command: ChangeStatusCommand[IO]
  ): ConnectionIO[UpdateResult] =
    queries
      .map(query => measureExecutionTime(query).map(query -> _))
      .sequence
      .map(_.foldLeft(Updated: UpdateResult) {
        case (Updated, (_, 1)) => Updated
        case (Updated, (query, result)) =>
          UpdateResult.Failure(
            Refined.unsafeApply(
              s"${query.name} failed for event ${command.eventId} to status ${command.status} with result $result. " +
                s"Rolling back queries: ${queries.map(_.name.toString).mkString(", ")}"
            )
          )
        case (other, (_, _)) => other
      })
}

object IOUpdateCommandsRunner {

  import cats.effect.IO

  def apply(transactor:       DbTransactor[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
            logger:           Logger[IO]
  ): IO[StatusUpdatesRunner[IO]] = IO {
    new StatusUpdatesRunnerImpl(transactor, queriesExecTimes, logger)
  }
}
