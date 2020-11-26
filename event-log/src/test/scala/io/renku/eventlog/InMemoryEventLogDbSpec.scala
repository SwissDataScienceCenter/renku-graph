/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import java.time.Instant

import ch.datascience.db.DbSpec
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.{BatchDate, CompoundEventId, EventBody, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.Path
import doobie.implicits._
import doobie.util.fragment.Fragment
import org.scalatest.TestSuite

trait InMemoryEventLogDbSpec extends DbSpec with InMemoryEventLogDb {
  self: TestSuite =>

  protected def initDb(): Unit = {
    execute {
      sql"""|CREATE TABLE IF NOT EXISTS event_log(
            | event_id varchar NOT NULL,
            | project_id int4 NOT NULL,
            | project_path varchar NOT NULL,
            | status varchar NOT NULL,
            | created_date timestamp NOT NULL,
            | execution_date timestamp NOT NULL,
            | event_date timestamp NOT NULL,
            | batch_date timestamp NOT NULL,
            | event_body text NOT NULL,
            | message varchar,
            | PRIMARY KEY (event_id, project_id)
            |);
       """.stripMargin.update.run.map(_ => ())
    }
    execute {
      sql"""|CREATE TABLE IF NOT EXISTS project(
            |project_id        int4      NOT NULL,
            |project_path      VARCHAR   NOT NULL,
            |latest_event_date timestamp NOT NULL,
            |PRIMARY KEY (project_id)
            |);
    """.stripMargin.update.run.map(_ => ())
    }
  }

  protected def prepareDbForTest(): Unit = Tables.all.foreach { tableName =>
    execute {
      Fragment.const(s"TRUNCATE TABLE $tableName").update.run.map(_ => ())
    }
  }

  protected def storeEvent(compoundEventId: CompoundEventId,
                           eventStatus:     EventStatus,
                           executionDate:   ExecutionDate,
                           eventDate:       EventDate,
                           eventBody:       EventBody,
                           createdDate:     CreatedDate = CreatedDate(Instant.now),
                           batchDate:       BatchDate = BatchDate(Instant.now),
                           projectPath:     Path = projectPaths.generateOne,
                           maybeMessage:    Option[EventMessage] = None
  ): Unit = {
    insertEvent(compoundEventId,
                eventStatus,
                executionDate,
                eventDate,
                eventBody,
                createdDate,
                batchDate,
                projectPath,
                maybeMessage
    )
    upsertProject(compoundEventId, projectPath, eventDate)
  }

  protected def insertEvent(compoundEventId: CompoundEventId,
                            eventStatus:     EventStatus,
                            executionDate:   ExecutionDate,
                            eventDate:       EventDate,
                            eventBody:       EventBody,
                            createdDate:     CreatedDate,
                            batchDate:       BatchDate,
                            projectPath:     Path,
                            maybeMessage:    Option[EventMessage]
  ): Unit = execute {
    maybeMessage match {
      case None =>
        sql"""|insert into
              |event_log (event_id, project_id, project_path, status, created_date, execution_date, event_date, batch_date, event_body)
              |values (${compoundEventId.id}, ${compoundEventId.projectId}, $projectPath, $eventStatus, $createdDate, $executionDate, $eventDate, $batchDate, $eventBody)
      """.stripMargin.update.run.map(_ => ())
      case Some(message) =>
        sql"""|insert into
              |event_log (event_id, project_id, project_path, status, created_date, execution_date, event_date, batch_date, event_body, message)
              |values (${compoundEventId.id}, ${compoundEventId.projectId}, $projectPath, $eventStatus, $createdDate, $executionDate, $eventDate, $batchDate, $eventBody, $message)
      """.stripMargin.update.run.map(_ => ())
    }
  }

  private def upsertProject(compoundEventId: CompoundEventId, projectPath: Path, eventDate: EventDate): Unit = execute {
    sql"""|insert into
          |project (project_id, project_path, latest_event_date)
          |values (${compoundEventId.projectId}, $projectPath, $eventDate)
          |on conflict (project_id)
          |do update set latest_event_date = excluded.latest_event_dat where excluded.latest_event_date > project.latest_event_date
      """.stripMargin.update.run.map(_ => ())
  }

  // format: off
  protected def findEvents(status:  EventStatus,
                           orderBy: Fragment = fr"created_date asc"): List[(CompoundEventId, ExecutionDate, BatchDate)] =
    execute {
      (fr"""select event_id, project_id, execution_date, batch_date
            from event_log
            where status = $status
            order by """ ++ orderBy)
        .query[(CompoundEventId, ExecutionDate, BatchDate)]
        .to[List]
    }
  // format: on

  protected def findProjects(): List[(projects.Id, projects.Path, EventDate)] = execute {
    sql"""select * from project"""
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
    execute {
      sql"""select message
            from event_log 
            where event_id = ${eventId.id} and project_id = ${eventId.projectId}"""
        .query[Option[EventMessage]]
        .unique
    }
}
