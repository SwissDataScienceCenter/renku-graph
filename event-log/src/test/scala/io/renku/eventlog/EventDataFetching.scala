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

package io.renku.eventlog

import cats.data.Kleisli
import io.renku.graph.model.events.{BatchDate, CompoundEventId, EventId, EventProcessingTime, EventStatus, ZippedEventPayload}
import io.renku.graph.model.projects
import skunk._
import skunk.implicits._

trait EventDataFetching {
  self: InMemoryEventLogDb =>

  protected def findEvents(status:  EventStatus,
                           orderBy: Fragment[Void] = sql"created_date asc"
  ): List[(CompoundEventId, ExecutionDate, BatchDate)] =
    execute {
      Kleisli { session =>
        val query: Query[(EventStatus, Void), (CompoundEventId, ExecutionDate, BatchDate)] = (sql"""
            SELECT event_id, project_id, execution_date, batch_date
            FROM event
            WHERE status = $eventStatusEncoder
            ORDER BY """ ~ orderBy)
          .query(eventIdDecoder ~ projectIdDecoder ~ executionDateDecoder ~ batchDateDecoder)
          .map { case eventId ~ projectId ~ executionDate ~ batchDate =>
            (CompoundEventId(eventId, projectId), executionDate, batchDate)
          }
        session.prepare(query).use(_.stream((status, Void), 32).compile.toList)
      }
    }

  protected def findPayload(eventId: CompoundEventId): Option[(CompoundEventId, ZippedEventPayload)] = execute {
    Kleisli { session =>
      val query: Query[EventId ~ projects.Id, (CompoundEventId, ZippedEventPayload)] =
        sql"""SELECT event_id, project_id, payload
              FROM event_payload
              WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder;"""
          .query(eventIdDecoder ~ projectIdDecoder ~ zippedPayloadDecoder)
          .map { case eventId ~ projectId ~ eventPayload =>
            (CompoundEventId(eventId, projectId), eventPayload)
          }
      session.prepare(query).use(_.option(eventId.id ~ eventId.projectId))
    }
  }

  protected def findProjects: List[(projects.Id, projects.Path, EventDate)] = execute {
    Kleisli { session =>
      val query: Query[Void, (projects.Id, projects.Path, EventDate)] =
        sql"""SELECT * FROM project"""
          .query(projectIdDecoder ~ projectPathDecoder ~ eventDateDecoder)
          .map { case projectId ~ projectPath ~ eventDate => (projectId, projectPath, eventDate) }
      session.execute(query)
    }
  }

  protected implicit class FoundEventsOps(events: List[(CompoundEventId, ExecutionDate, BatchDate)]) {
    lazy val noBatchDate: List[(CompoundEventId, ExecutionDate)] = events.map { case (id, executionDate, _) =>
      id -> executionDate
    }
    lazy val eventIdsOnly: List[CompoundEventId] = events.map { case (id, _, _) => id }
  }

  protected def findEventMessage(eventId: CompoundEventId): Option[EventMessage] =
    findEvent(eventId).flatMap(_._3)

  protected def findEvent(eventId: CompoundEventId): Option[(ExecutionDate, EventStatus, Option[EventMessage])] =
    execute {
      Kleisli { session =>
        val query: Query[EventId ~ projects.Id, (ExecutionDate, EventStatus, Option[EventMessage])] = sql"""
          SELECT execution_date, status, message
          FROM event
          WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
        """.query(executionDateDecoder ~ eventStatusDecoder ~ eventMessageDecoder.opt).map {
          case executionDate ~ eventStatus ~ maybeEventMessage => (executionDate, eventStatus, maybeEventMessage)
        }
        session.prepare(query).use(_.option(eventId.id ~ eventId.projectId))
      }
    }

  protected def findProcessingTime(eventId: CompoundEventId): List[(CompoundEventId, EventProcessingTime)] =
    execute {
      Kleisli { session =>
        val query: Query[EventId ~ projects.Id, (CompoundEventId, EventProcessingTime)] = sql"""
          SELECT event_id, project_id, processing_time
          FROM status_processing_time
          WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder;
        """
          .query(eventIdDecoder ~ projectIdDecoder ~ eventProcessingTimeDecoder)
          .map { case eventId ~ projectId ~ eventProcessingTime =>
            (CompoundEventId(eventId, projectId), eventProcessingTime)
          }
        session.prepare(query).use(_.stream(eventId.id ~ eventId.projectId, 32).compile.toList)
      }
    }

  protected implicit class FoundEventsProcessingTimeOps(events: List[(CompoundEventId, EventProcessingTime)]) {
    lazy val eventIdsOnly: List[CompoundEventId] = events.map { case (id, _) => id }
  }
}
