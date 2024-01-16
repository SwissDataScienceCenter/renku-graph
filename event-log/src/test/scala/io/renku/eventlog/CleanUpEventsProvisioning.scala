/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import cats.effect.IO
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.events.consumers
import io.renku.graph.model.projects
import skunk._
import skunk.codec.all.timestamptz
import skunk.implicits._

import java.time.OffsetDateTime

trait CleanUpEventsProvisioning {
  self: EventLogPostgresSpec with TypeSerializers =>

  protected def insertCleanUpEvent(project: consumers.Project, date: OffsetDateTime = OffsetDateTime.now())(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[Unit] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Command[OffsetDateTime *: projects.GitLabId *: projects.Slug *: EmptyTuple] = sql"""
          INSERT INTO clean_up_events_queue (date, project_id, project_slug)
          VALUES ($timestamptz, $projectIdEncoder, $projectSlugEncoder)""".command
      session
        .prepare(query)
        .flatMap(_.execute(date *: project.id *: project.slug *: EmptyTuple))
        .void
    }

  protected def findCleanUpEvents(implicit cfg: DBConfig[EventLogDB]): IO[List[consumers.Project]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[Void, consumers.Project] = sql"""
        SELECT project_id, project_slug
        FROM clean_up_events_queue
        ORDER BY date DESC"""
        .query(projectIdDecoder ~ projectSlugDecoder)
        .map { case (id: projects.GitLabId) ~ (slug: projects.Slug) => consumers.Project(id, slug) }
      session.prepare(query).flatMap(_.stream(Void, 32).compile.toList)
    }
}
