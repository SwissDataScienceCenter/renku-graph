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
import cats.effect.{BracketThrow, Sync}
import cats.syntax.all._
import ch.datascience.db.implicits._
import ch.datascience.db.{DbClient, SqlStatement}
import ch.datascience.graph.model.events.{EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.ExecutionDate
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.ToTriplesGenerated
import skunk.data.Completion
import skunk.implicits._
import skunk.{Session, ~}

import java.time.Instant

private class ToTriplesGeneratedUpdater[Interpretation[_]: BracketThrow: Sync](
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with DBUpdater[Interpretation, ToTriplesGenerated] {

  private lazy val partitionSize = 50

  override def updateDB(
      event: ToTriplesGenerated
  ): UpdateResult[Interpretation] = for {
    _              <- updateEvent(event)
    idsAndStatuses <- updateAncestorsStatus(event)
    _              <- cleanUp(idsAndStatuses, event)
  } yield DBUpdateResults.ForProject(event.projectPath, idsAndStatuses.groupBy(s => s._2).view.mapValues(_.size).toMap)

  private def updateEvent(event: ToTriplesGenerated) = for {
    _ <- updateStatus(event)
    _ <- updatePayload(event)
    _ <- updateProcessingTime(event)
  } yield ()

  private def updateStatus(event: ToTriplesGenerated) = measureExecutionTime {
    SqlStatement(name = "status_change_event - triples_generated_status")
      .command[ExecutionDate ~ EventId ~ projects.Id](
        sql"""UPDATE event evt
          SET status = '#${EventStatus.TriplesGenerated.value}', 
            execution_date = $executionDateEncoder, 
            message = NULL
          WHERE evt.event_id = $eventIdEncoder AND evt.project_id = $projectIdEncoder AND evt.status = '#${EventStatus.GeneratingTriples.value}'""".command
      )
      .arguments(ExecutionDate(now()) ~ event.eventId.id ~ event.eventId.projectId)
      .build
      .flatMapResult {
        case Completion.Update(1) => ().pure[Interpretation]
        case _ =>
          new Exception(s"Could not update event ${event.eventId} to status ${EventStatus.TriplesGenerated} ")
            .raiseError[Interpretation, Unit]
      }
  }

  private def updatePayload(event: ToTriplesGenerated) = measureExecutionTime {
    SqlStatement(name = "status_change_event - triples_generated_payload")
      .command(
        sql"""INSERT INTO event_payload (event_id, project_id, payload, schema_version)
            VALUES ($eventIdEncoder,  $projectIdEncoder, $eventPayloadEncoder, $schemaVersionEncoder)
            ON CONFLICT (event_id, project_id, schema_version)
            DO UPDATE SET payload = EXCLUDED.payload;""".command
      )
      .arguments(event.eventId.id ~ event.eventId.projectId ~ event.payload ~ event.schemaVersion)
      .build
      .void
  }

  private def updateProcessingTime(event: ToTriplesGenerated) = measureExecutionTime {
    SqlStatement(name = "status_change_event - triples_generated_processing_time")
      .command[EventId ~ projects.Id ~ EventStatus ~ EventProcessingTime](
        sql"""INSERT INTO status_processing_time(event_id, project_id, status, processing_time)
                VALUES($eventIdEncoder, $projectIdEncoder, $eventStatusEncoder, $eventProcessingTimeEncoder)
                ON CONFLICT (event_id, project_id, status)
                DO UPDATE SET processing_time = EXCLUDED.processing_time;
                """.command
      )
      .arguments(event.eventId.id ~ event.eventId.projectId ~ EventStatus.TriplesGenerated ~ event.processingTime)
      .build
      .void
  }

  private def updateAncestorsStatus(event: ToTriplesGenerated) = measureExecutionTime {
    SqlStatement(name = "status_change_event - triples_generated_ancestors")
      .select[ExecutionDate ~ projects.Id ~ projects.Id ~ EventId ~ EventId, EventId ~ EventStatus](
        sql"""UPDATE event evt
              SET status = '#${EventStatus.TriplesGenerated.value}', 
                  execution_date = $executionDateEncoder, 
                  message = NULL
              FROM (
                SELECT event_id, project_id, status 
                FROM event
                WHERE project_id = $projectIdEncoder
                  AND #${`status IN`(EventStatus.all.filterNot(_ == EventStatus.Skipped))} 
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
              RETURNING evt.event_id, old_evt.status
           """.query(eventIdDecoder ~ eventStatusDecoder)
      )
      .arguments(
        ExecutionDate(now()) ~ event.eventId.projectId ~ event.eventId.projectId ~ event.eventId.id ~ event.eventId.id
      )
      .build(_.toList)

  }

  private def cleanUp(idsAndStatuses: List[(EventId, EventStatus)],
                      event:          ToTriplesGenerated
  ): Kleisli[Interpretation, Session[Interpretation], Unit] = Kleisli { session =>
    idsAndStatuses
      .sliding(size = partitionSize, step = partitionSize)
      .map(executeRemovalQueries(event)(_).run(session))
      .toList
      .sequence
      .void
  }

  private def executeRemovalQueries(
      event:      ToTriplesGenerated
  )(eventsWindow: List[(EventId, EventStatus)]): Kleisli[Interpretation, Session[Interpretation], Unit] = for {
    _ <- removeAncestorsProcessingTimes(eventsWindow, event.eventId.projectId)
    _ <- removeAncestorsPayloads(eventsWindow, event.eventId.projectId)
    _ <- removeAwaitingDeletionEvents(eventsWindow, event.eventId.projectId)
  } yield ()

  private def removeAncestorsPayloads(idsAndStatuses: List[(EventId, EventStatus)], projectId: projects.Id) =
    idsAndStatuses.filterNot { case (_, status) =>
      Set(EventStatus.New, EventStatus.GeneratingTriples, EventStatus.Skipped).contains(status)
    } match {
      case Nil => Kleisli.pure(())
      case eventIdsToRemove =>
        measureExecutionTime {
          SqlStatement(name = "status_change_event - payload_removal")
            .command[projects.Id](
              sql"""DELETE FROM event_payload
                    WHERE event_id IN (#${eventIdsToRemove.map { case (id, _) => s"'$id'" }.mkString(",")})
                      AND project_id = $projectIdEncoder""".command
            )
            .arguments(projectId)
            .build
            .void
        }
    }

  private def removeAncestorsProcessingTimes(idsAndStatuses: List[(EventId, EventStatus)], projectId: projects.Id) =
    idsAndStatuses.filterNot { case (_, status) =>
      Set(EventStatus.New, EventStatus.GeneratingTriples, EventStatus.Skipped).contains(status)
    } match {
      case Nil => Kleisli.pure(())
      case eventIdsToRemove =>
        measureExecutionTime {
          SqlStatement(name = "status_change_event - processing_time_removal")
            .command[projects.Id](
              sql"""DELETE FROM status_processing_time
                    WHERE event_id IN (#${eventIdsToRemove.map { case (id, _) => s"'$id'" }.mkString(",")})
                      AND project_id = $projectIdEncoder""".command
            )
            .arguments(projectId)
            .build
            .void
        }
    }

  private def removeAwaitingDeletionEvents(idsAndStatuses: List[(EventId, EventStatus)], projectId: projects.Id) =
    idsAndStatuses.collect { case (id, EventStatus.AwaitingDeletion) => id } match {
      case Nil => Kleisli.pure(())
      case eventIdsToRemove =>
        measureExecutionTime {
          SqlStatement(name = "status_change_event - awaiting_deletion_event_removal")
            .command[projects.Id](
              sql"""DELETE FROM event
                    WHERE event_id IN (#${eventIdsToRemove.map(id => s"'$id'").mkString(",")})
                      AND project_id = $projectIdEncoder""".command
            )
            .arguments(projectId)
            .build
            .void
        }
    }

  private def `status IN`(statuses: Set[EventStatus]) =
    s"status IN (${statuses.map(s => s"'$s'").toList.mkString(",")})"
}
