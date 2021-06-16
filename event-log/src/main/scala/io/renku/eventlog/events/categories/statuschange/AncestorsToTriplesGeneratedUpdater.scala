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
import ch.datascience.graph.model.events.{EventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.ExecutionDate
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.AncestorsToTriplesGenerated
import skunk.implicits._
import skunk.{Session, ~}

import java.time.Instant

private class AncestorsToTriplesGeneratedUpdater[Interpretation[_]: BracketThrow: Sync](
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with DBUpdater[Interpretation, AncestorsToTriplesGenerated] {

  private lazy val partitionSize = 50

  override def updateDB(
      event: AncestorsToTriplesGenerated
  ): UpdateResult[Interpretation] = for {
    idsAndStatuses <- updateAncestorsStatus(event)
    _              <- cleanUp(idsAndStatuses, event)
  } yield DBUpdateResults(event.projectPath, idsAndStatuses.groupBy(s => s._2).view.mapValues(_.size).toMap)

  private def cleanUp(idsAndStatuses: List[(EventId, EventStatus)],
                      event:          AncestorsToTriplesGenerated
  ): Kleisli[Interpretation, Session[Interpretation], Unit] = Kleisli { session =>
    idsAndStatuses
      .sliding(size = partitionSize, step = partitionSize)
      .map(executeRemovalQueries(event)(_).run(session))
      .toList
      .sequence
      .void
  }

  private def executeRemovalQueries(
      event:      AncestorsToTriplesGenerated
  )(eventsWindow: List[(EventId, EventStatus)]): Kleisli[Interpretation, Session[Interpretation], Unit] = Kleisli {
    session =>
      List(
        removeAncestorsProcessingTimes(eventsWindow, event.eventId.projectId).run(session),
        removeAncestorsPayloads(eventsWindow, event.eventId.projectId).run(session),
        removeAwaitingDeletionEvents(eventsWindow, event.eventId.projectId).run(session)
      ).sequence.void
  }

  private def updateAncestorsStatus(event: AncestorsToTriplesGenerated) = measureExecutionTime {
    SqlStatement(name = "status_change_event - triples_generated")
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
