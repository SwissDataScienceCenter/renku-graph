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

package io.renku.eventlog.events.categories.statuschange

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.effect.kernel.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import io.renku.db.implicits._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.ToFailure
import io.renku.eventlog.{EventMessage, ExecutionDate}
import io.renku.graph.model.events.EventId
import io.renku.graph.model.events.EventStatus.{FailureStatus, New, ProcessingStatus, TransformationNonRecoverableFailure, TriplesGenerated}
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import skunk.data.Completion
import skunk.implicits._
import skunk.~

import java.time.Instant

private class ToFailureUpdater[Interpretation[_]: MonadCancelThrow: Async](
    deliveryInfoRemover: DeliveryInfoRemover[Interpretation],
    queriesExecTimes:    LabeledHistogram[Interpretation, SqlStatement.Name],
    now:                 () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with DBUpdater[Interpretation, ToFailure[ProcessingStatus, FailureStatus]] {

  import deliveryInfoRemover._

  override def updateDB(event: ToFailure[ProcessingStatus, FailureStatus]): UpdateResult[Interpretation] = for {
    _                     <- deleteDelivery(event.eventId)
    eventUpdateResult     <- updateEvent(event)
    ancestorsUpdateResult <- maybeUpdateAncestors(event)
  } yield ancestorsUpdateResult combine eventUpdateResult

  override def onRollback(event: ToFailure[ProcessingStatus, FailureStatus]) = deleteDelivery(event.eventId)

  private def updateEvent(event: ToFailure[ProcessingStatus, FailureStatus]) = measureExecutionTime {
    SqlStatement[Interpretation](name = Refined.unsafeApply(s"to_${event.newStatus.value.toLowerCase} - status update"))
      .command[FailureStatus ~ ExecutionDate ~ EventMessage ~ EventId ~ projects.Id ~ ProcessingStatus](
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
          ExecutionDate(now()) ~
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
            .pure[Interpretation]
        case _ =>
          new Exception(s"Could not update event ${event.eventId} to status ${event.newStatus}")
            .raiseError[Interpretation, DBUpdateResults.ForProjects]
      }
  }

  private def maybeUpdateAncestors(event: ToFailure[ProcessingStatus, FailureStatus]) = event.newStatus match {
    case TransformationNonRecoverableFailure => updateAncestorsStatus(event)
    case _                                   => Kleisli.pure(DBUpdateResults.ForProjects(event.projectPath, Map.empty))
  }

  private def updateAncestorsStatus(event: ToFailure[ProcessingStatus, FailureStatus]) = measureExecutionTime {
    SqlStatement(name = Refined.unsafeApply(s"to_${event.newStatus.value.toLowerCase} - ancestors update"))
      .select[ExecutionDate ~ projects.Id ~ projects.Id ~ EventId ~ EventId, EventId](
        sql"""UPDATE event evt
              SET status = '#${New.value}', 
                  execution_date = $executionDateEncoder, 
                  message = NULL
              FROM (
                SELECT event_id, project_id 
                FROM event
                WHERE project_id = $projectIdEncoder
                  AND status = '#${TriplesGenerated.value}'
                  AND event_date <= (
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
        ExecutionDate(now()) ~ event.eventId.projectId ~ event.eventId.projectId ~ event.eventId.id ~ event.eventId.id
      )
      .build(_.toList)
      .mapResult { ids =>
        DBUpdateResults.ForProjects(event.projectPath, Map(New -> ids.size, TriplesGenerated -> -ids.size))
      }
  }
}
