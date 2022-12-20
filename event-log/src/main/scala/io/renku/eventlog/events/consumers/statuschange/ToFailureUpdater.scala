/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.statuschange

import cats.data.Kleisli
import cats.effect.Temporal
import cats.effect.kernel.Async
import cats.syntax.all._
import io.renku.db.implicits._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.events.consumers.statuschange.StatusChangeEvent.ToFailure
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.model.events.EventStatus.{FailureStatus, New, ProcessingStatus, TransformationNonRecoverableFailure, TransformationRecoverableFailure, TriplesGenerated}
import io.renku.graph.model.events.{EventId, EventMessage, EventStatus, ExecutionDate}
import io.renku.graph.model.projects
import org.typelevel.log4cats.Logger
import skunk.SqlState.DeadlockDetected
import skunk.data.Completion
import skunk.implicits._
import skunk.{Session, ~}

import java.time.{Duration, Instant}
import scala.concurrent.duration._

private class ToFailureUpdater[F[_]: Async: Logger: QueriesExecutionTimes](
    deliveryInfoRemover: DeliveryInfoRemover[F],
    now:                 () => Instant = () => Instant.now
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with DBUpdater[F, ToFailure[ProcessingStatus, FailureStatus]] {

  import deliveryInfoRemover._

  override def onRollback(event: ToFailure[ProcessingStatus, FailureStatus]) = deleteDelivery(event.eventId)

  override def updateDB(event: ToFailure[ProcessingStatus, FailureStatus]): UpdateResult[F] = for {
    _                     <- deleteDelivery(event.eventId)
    eventUpdateResult     <- updateEvent(event)
    ancestorsUpdateResult <- maybeUpdateAncestors(event, eventUpdateResult)
  } yield ancestorsUpdateResult combine eventUpdateResult

  private def updateEvent(
      event: ToFailure[ProcessingStatus, FailureStatus]
  ): Kleisli[F, Session[F], DBUpdateResults.ForProjects] = measureExecutionTime {
    SqlStatement
      .named(s"to_${event.newStatus.value.toLowerCase} - status update")
      .command[FailureStatus ~ ExecutionDate ~ EventMessage ~ EventId ~ projects.GitLabId ~ ProcessingStatus](
        sql"""UPDATE event
              SET status = $eventFailureStatusEncoder,
                execution_date = $executionDateEncoder,
                message = $eventMessageEncoder
              WHERE event_id = $eventIdEncoder 
                AND project_id = $projectIdEncoder 
                AND status = $eventProcessingStatusEncoder
               """.command
      )
      .arguments(
        event.newStatus ~
          ExecutionDate(now().plusMillis(event.maybeExecutionDelay.getOrElse(Duration.ofMillis(0)).toMillis)) ~
          event.message ~
          event.eventId.id ~
          event.eventId.projectId ~
          event.currentStatus
      )
      .build
      .flatMapResult {
        case Completion.Update(1) =>
          DBUpdateResults
            .ForProjects(event.projectPath, Map(event.currentStatus -> -1, event.newStatus -> 1))
            .pure[F]
        case Completion.Update(0) => DBUpdateResults.ForProjects.empty.pure[F]
        case completion =>
          new Exception(s"Could not update event ${event.eventId} to status ${event.newStatus}: $completion")
            .raiseError[F, DBUpdateResults.ForProjects]
      }
  }.recoverWith(retryUpdating(event))

  private def retryUpdating(
      event: ToFailure[ProcessingStatus, FailureStatus]
  ): PartialFunction[Throwable, Kleisli[F, Session[F], DBUpdateResults.ForProjects]] = { case DeadlockDetected(_) =>
    Kleisli.liftF[F, Session[F], Unit] {
      Logger[F].warn(show"Deadlock while updating event ${event.eventId} to ${event.newStatus}") >>
        Temporal[F].sleep(1 second)
    } >> updateEvent(event)
  }

  private def maybeUpdateAncestors(event:         ToFailure[ProcessingStatus, FailureStatus],
                                   updateResults: DBUpdateResults.ForProjects
  ) = updateResults -> event.newStatus match {
    case (results @ DBUpdateResults.ForProjects.empty, _) => Kleisli.pure(results)
    case (_, TransformationNonRecoverableFailure)         => updateAncestorsStatus(event, New)
    case (_, TransformationRecoverableFailure) => updateAncestorsStatus(event, TransformationRecoverableFailure)
    case _                                     => Kleisli.pure(DBUpdateResults.ForProjects.empty)
  }

  private def updateAncestorsStatus(event: ToFailure[ProcessingStatus, FailureStatus], newStatus: EventStatus) =
    measureExecutionTime {
      SqlStatement
        .named(s"to_${event.newStatus.value.toLowerCase} - ancestors update")
        .select[EventStatus ~ ExecutionDate ~ projects.GitLabId ~ projects.GitLabId ~ EventId ~ EventId, EventId](
          sql"""UPDATE event evt
                SET status = $eventStatusEncoder, 
                    execution_date = $executionDateEncoder, 
                    message = NULL
                FROM (
                  SELECT event_id, project_id 
                  FROM event
                  WHERE project_id = $projectIdEncoder
                    AND status = '#${TriplesGenerated.value}'
                    AND event_date < (
                      SELECT event_date
                      FROM event
                      WHERE project_id = $projectIdEncoder
                        AND event_id = $eventIdEncoder
                    )
                    AND event_id <> $eventIdEncoder
                  FOR UPDATE
                ) old_evt
                WHERE evt.event_id = old_evt.event_id AND evt.project_id = old_evt.project_id 
                RETURNING evt.event_id
           """.query(eventIdDecoder)
        )
        .arguments(
          newStatus ~
            ExecutionDate(
              now().plusMillis(event.maybeExecutionDelay.getOrElse(Duration ofMillis 0).toMillis)
            ) ~ event.eventId.projectId ~ event.eventId.projectId ~ event.eventId.id ~ event.eventId.id
        )
        .build(_.toList)
        .mapResult { ids =>
          DBUpdateResults.ForProjects(event.projectPath, Map(newStatus -> ids.size, TriplesGenerated -> -ids.size))
        }
    }
}
