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
import ch.datascience.data.ErrorMessage
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import org.typelevel.log4cats.Logger
import io.renku.eventlog.EventLogDB
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
          _ <- deleteDelivery(command) recoverWith errorToUpdateResult(
                 s"Event ${command.eventId} - cannot remove event delivery",
                 Completion.Delete(0).pure[Interpretation].widen[Completion]
               )
          sp <- transaction.savepoint
          updateResult <-
            executeCommand(command) recoverWith errorToUpdateResult(s"Event ${command.eventId} got ${command.status}",
                                                                    transaction.rollback(sp)
            )
          _ <- logInfo(command, updateResult)
          _ <- command updateGauges updateResult
        } yield updateResult
      }
  }

  private def errorToUpdateResult(message:  String,
                                  rollback: => Interpretation[Completion]
  ): PartialFunction[Throwable, Interpretation[UpdateResult]] = { case NonFatal(exception) =>
    for {
      _ <- rollback
      _ <- logger.error(exception)(message)
    } yield UpdateResult.Failure(ErrorMessage.withExceptionMessage(exception))
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
        runUpdateQueriesIfSuccessful(
          command.queries.toList :++ maybeUpdateProcessingTimeQuery(command),
          command
        ) >>= toUpdateResult
      case false => NotFound.pure[Interpretation].widen[commands.UpdateResult]
    }

  private def deleteDelivery(command: ChangeStatusCommand[Interpretation])(implicit session: Session[Interpretation]) =
    measureExecutionTime {
      SqlQuery(
        Kleisli { session =>
          val query: Command[EventId ~ projects.Id] = sql"""DELETE FROM event_delivery
                            WHERE event_id = $eventIdPut AND project_id = $projectIdPut
                         """.command
          session
            .prepare(query)
            .use(_.execute(command.eventId.id ~ command.eventId.projectId).map(_ => UpdateResult.Updated: UpdateResult))
        },
        name = "status update - delivery info remove"
      )
    }

  private def toUpdateResult: UpdateResult => Interpretation[UpdateResult] = {
    case UpdateResult.Failure(message) => new Exception(message).raiseError[Interpretation, UpdateResult]
    case result                        => result.pure[Interpretation]
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

  private def maybeUpdateProcessingTimeQuery(command: ChangeStatusCommand[Interpretation]) =
    upsertStatusProcessingTime(command.eventId, command.status, command.maybeProcessingTime)

  private def runUpdateQueriesIfSuccessful(queries: List[SqlQuery[Interpretation, Int]],
                                           command: ChangeStatusCommand[Interpretation]
  )(implicit session:                               Session[Interpretation]): Interpretation[UpdateResult] =
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

  def apply(transactor:       SessionResource[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
            logger:           Logger[IO]
  ): IO[StatusUpdatesRunner[IO]] = IO {
    new StatusUpdatesRunnerImpl(transactor, queriesExecTimes, logger)
  }
}
