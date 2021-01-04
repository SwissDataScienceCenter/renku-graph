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

import ch.datascience.graph.model.events.{BatchDate, CompoundEventId, EventStatus}
import ch.datascience.graph.model.projects
import doobie.util.fragment.Fragment
import doobie.implicits._

trait EventLogDataFetching {
  self: InMemoryEventLogDb =>
  // format: off
  protected def findEvents(status:  EventStatus,
                           orderBy: Fragment = fr"created_date asc"): List[(CompoundEventId, ExecutionDate, BatchDate)] =
    execute {
      (fr"""SELECT event_id, project_id, execution_date, batch_date
            FROM event
            WHERE status = $status
            ORDER BY """ ++ orderBy)
        .query[(CompoundEventId, ExecutionDate, BatchDate)]
        .to[List]
    }
  // format: on
  protected def findPayload(eventId: CompoundEventId): Option[(CompoundEventId, EventPayload)] =
    execute {
      (fr"""SELECT event_id, project_id, payload
            FROM event_payload
            WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId};""")
        .query[(CompoundEventId, EventPayload)]
        .option
    }

  protected def findProjects(): List[(projects.Id, projects.Path, EventDate)] = execute {
    sql"""SELECT * FROM project"""
      .query[(projects.Id, projects.Path, EventDate)]
      .to[List]
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
      sql"""|SELECT execution_date, status, message
            |FROM event 
            |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId}
         """.stripMargin
        .query[(ExecutionDate, EventStatus, Option[EventMessage])]
        .option
    }
}
