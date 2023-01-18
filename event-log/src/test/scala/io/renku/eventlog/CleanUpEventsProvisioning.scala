/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import io.renku.events.consumers
import io.renku.graph.model.projects
import skunk._
import skunk.codec.all.timestamptz
import skunk.implicits._

import java.time.OffsetDateTime

trait CleanUpEventsProvisioning {
  self: InMemoryEventLogDb =>

  protected def insertCleanUpEvent(project: consumers.Project, date: OffsetDateTime = OffsetDateTime.now()): Unit =
    insertCleanUpEvent(project.id, project.path, date)

  protected def insertCleanUpEvent(projectId:   projects.GitLabId,
                                   projectPath: projects.Path,
                                   date:        OffsetDateTime
  ): Unit =
    execute {
      Kleisli { session =>
        val query: Command[OffsetDateTime ~ projects.GitLabId ~ projects.Path] = sql"""
          INSERT INTO clean_up_events_queue (date, project_id, project_path)
          VALUES ($timestamptz, $projectIdEncoder, $projectPathEncoder)""".command
        session
          .prepare(query)
          .flatMap(_.execute(date ~ projectId ~ projectPath))
          .void
      }
    }

  protected def findCleanUpEvents: List[(projects.GitLabId, projects.Path)] = execute {
    Kleisli { session =>
      val query: Query[Void, projects.GitLabId ~ projects.Path] = sql"""
        SELECT project_id, project_path
        FROM clean_up_events_queue
        ORDER BY date DESC"""
        .query(projectIdDecoder ~ projectPathDecoder)
      session.prepare(query).flatMap(_.stream(Void, 32).compile.toList)
    }
  }
}
