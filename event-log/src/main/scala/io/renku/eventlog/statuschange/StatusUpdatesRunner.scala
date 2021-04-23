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
import cats.effect.{Async, Bracket}
import cats.syntax.all._
import ch.datascience.data.ErrorMessage
import ch.datascience.db.{DbClient, SessionResource, SqlStatement}
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.statuschange.commands.UpdateResult.{NotFound, Updated}
import io.renku.eventlog.statuschange.commands.{ChangeStatusCommand, UpdateResult}
import org.typelevel.log4cats.Logger
import skunk._
import skunk.data.Completion
import skunk.implicits._

import scala.util.control.NonFatal

trait StatusUpdatesRunner[Interpretation[_]] {
  def run(command: ChangeStatusCommand[Interpretation]): Interpretation[UpdateResult]
}

class StatusUpdatesRunnerImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    logger:           Logger[Interpretation]
) extends DbClient(Some(queriesExecTimes))
    with StatusUpdatesRunner[Interpretation]
    with StatusProcessingTime {

  override def run(command: ChangeStatusCommand[Interpretation]): Interpretation[UpdateResult] =
    sessionResource.useWithTransactionK {
      Kleisli { case (transaction, session) =>
        {
          for {
            sp <- Kleisli.liftF(transaction.savepoint)
            updateResult <- (deleteDelivery(command) >> executeCommand(command)).recoverWith(
                              deliveryCleanUp(command, () => transaction.rollback(sp))
                            )
            _ <- Kleisli.liftF(logInfo(command, updateResult))
            _ <- command updateGauges updateResult
          } yield updateResult
        }.run(session)
      }
    }

  private def deliveryCleanUp(command:  ChangeStatusCommand[Interpretation],
                              rollback: () => Interpretation[Completion]
  ): PartialFunction[Throwable, Kleisli[Interpretation, Session[Interpretation], UpdateResult]] = {
    case NonFatal(exception) =>
      Kleisli.liftF[Interpretation, Session[Interpretation], Completion](rollback()) >>
        deleteDelivery(command)
          .recoverWith(deliveryErrorToResult(command)) >>= {
        case UpdateResult.Updated => logAndAsResult(s"Event ${command.eventId} got ${command.status}", exception)
        case deletionResult       => Kleisli.pure(deletionResult)
      }
  }

  private def deliveryErrorToResult(
      command: ChangeStatusCommand[Interpretation]
  ): PartialFunction[Throwable, Kleisli[Interpretation, Session[Interpretation], UpdateResult]] = {
    case NonFatal(exception) =>
      logAndAsResult(s"Event ${command.eventId} - cannot remove event delivery", exception)
  }

  private def logAndAsResult(message: String, cause: Throwable) =
    Kleisli.liftF(
      logger.error(cause)(message) >> UpdateResult
        .Failure(ErrorMessage.withExceptionMessage(cause))
        .pure[Interpretation]
        .widen[UpdateResult]
    )

  private def executeCommand(
      command: ChangeStatusCommand[Interpretation]
  ): Kleisli[Interpretation, Session[Interpretation], UpdateResult] =
    checkIfPersisted(command.eventId) >>= {
      case true =>
        executeQueries(
          queries = command.queries.toList :++ maybeUpdateProcessingTimeQuery(command),
          command
        ) >>= toUpdateResult
      case false => Kleisli.pure(NotFound).widen[commands.UpdateResult]
    }

  private def deleteDelivery(command: ChangeStatusCommand[Interpretation]) =
    measureExecutionTime {
      SqlStatement(name = "status update - delivery info remove")
        .command[EventId ~ projects.Id](
          sql"""DELETE FROM event_delivery
                  WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
               """.command
        )
        .arguments(command.eventId.id ~ command.eventId.projectId)
        .build
        .mapResult(_ => UpdateResult.Updated: UpdateResult)
    }

  private def toUpdateResult: UpdateResult => Kleisli[Interpretation, Session[Interpretation], UpdateResult] = {
    case UpdateResult.Failure(message) => Kleisli.liftF(new Exception(message).raiseError[Interpretation, UpdateResult])
    case result                        => Kleisli.pure(result)
  }

  private def checkIfPersisted(eventId: CompoundEventId) =
    measureExecutionTime {
      SqlStatement(name = "event update check existence")
        .select[EventId ~ projects.Id, EventId](
          sql"""SELECT event_id
                  FROM event
                  WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder"""
            .query(eventIdDecoder)
        )
        .arguments(eventId.id ~ eventId.projectId)
        .build(_.option)
        .mapResult(_.isDefined)
    }

  private def logInfo(command: ChangeStatusCommand[Interpretation], updateResult: UpdateResult) = updateResult match {
    case Updated => logger.info(s"Event ${command.eventId} got ${command.status}")
    case _       => Bracket[Interpretation, Throwable].unit
  }
  private def maybeUpdateProcessingTimeQuery(command: ChangeStatusCommand[Interpretation]) =
    upsertStatusProcessingTime(command.eventId, command.status, command.maybeProcessingTime)

  private def executeQueries(queries: List[SqlStatement[Interpretation, Int]],
                             command: ChangeStatusCommand[Interpretation]
  ) =
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

  def apply(sessionResource:  SessionResource[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlStatement.Name],
            logger:           Logger[IO]
  ): IO[StatusUpdatesRunner[IO]] = IO {
    new StatusUpdatesRunnerImpl(sessionResource, queriesExecTimes, logger)
  }
}
