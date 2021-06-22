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

import cats.effect.{BracketThrow, Sync}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SqlStatement}
import ch.datascience.graph.model.events.{EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.ExecutionDate
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.events.categories.statuschange.DBUpdateResults.ForProject
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.AncestorsToTriplesStore
import skunk.data.Completion
import skunk.implicits._
import skunk.~

import java.time.Instant

private class AncestorsToTriplesStoreUpdater[Interpretation[_]: BracketThrow: Sync](
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with DBUpdater[Interpretation, AncestorsToTriplesStore] {

  override def updateDB(
      event: AncestorsToTriplesStore
  ): UpdateResult[Interpretation] = for {
    _            <- updateEvent(event)
    updatedCount <- updateAncestorsStatus(event)
  } yield ForProject(event.projectPath, Map(EventStatus.TriplesGenerated -> updatedCount))

  private def updateEvent(event: AncestorsToTriplesStore) = for {
    _ <- updateStatus(event)
    _ <- updateProcessingTime(event)
  } yield ()

  private def updateStatus(event: AncestorsToTriplesStore) = measureExecutionTime {
    SqlStatement(name = "status_change_event - triples_store_status")
      .command[ExecutionDate ~ EventId ~ projects.Id](
        sql"""UPDATE event evt
          SET status = '#${EventStatus.TriplesStore.value}',
            execution_date = $executionDateEncoder,
            message = NULL
          WHERE evt.event_id = $eventIdEncoder AND evt.project_id = $projectIdEncoder AND evt.status = '#${EventStatus.TransformingTriples.value}'""".command
      )
      .arguments(ExecutionDate(now()) ~ event.eventId.id ~ event.eventId.projectId)
      .build
      .flatMapResult {
        case Completion.Update(1) => ().pure[Interpretation]
        case _ =>
          new Exception(s"Could not update event ${event.eventId} to status ${EventStatus.TriplesStore}")
            .raiseError[Interpretation, Unit]
      }
  }

  private def updateProcessingTime(event: AncestorsToTriplesStore) = measureExecutionTime {
    SqlStatement(name = "status_change_event - triples_store_processing_time")
      .command[EventId ~ projects.Id ~ EventStatus ~ EventProcessingTime](
        sql"""INSERT INTO status_processing_time(event_id, project_id, status, processing_time)
                VALUES($eventIdEncoder, $projectIdEncoder, $eventStatusEncoder, $eventProcessingTimeEncoder)
                ON CONFLICT (event_id, project_id, status)
                DO UPDATE SET processing_time = EXCLUDED.processing_time;
                """.command
      )
      .arguments(event.eventId.id ~ event.eventId.projectId ~ EventStatus.TriplesStore ~ event.processingTime)
      .build
      .mapResult(_ => 1)
  }

  private def updateAncestorsStatus(event: AncestorsToTriplesStore) = measureExecutionTime {
    SqlStatement(name = "status_change_event - triples_store_ancestors")
      .command[ExecutionDate ~ projects.Id ~ projects.Id ~ EventId ~ EventId](
        sql"""UPDATE event evt
              SET status = '#${EventStatus.TriplesStore.value}',
                  execution_date = $executionDateEncoder,
                  message = NULL
              WHERE project_id = $projectIdEncoder
                AND status = '#${EventStatus.TriplesGenerated.value}'
                AND event_date <= (
                  SELECT event_date
                  FROM event
                  WHERE project_id = $projectIdEncoder
                    AND event_id = $eventIdEncoder
                )
                AND event_id <> $eventIdEncoder
           """.command
      )
      .arguments(
        ExecutionDate(now()) ~ event.eventId.projectId ~ event.eventId.projectId ~ event.eventId.id ~ event.eventId.id
      )
      .build
      .mapResult {
        case Completion.Update(count) => count
        case _                        => 0
      }

  }

}
