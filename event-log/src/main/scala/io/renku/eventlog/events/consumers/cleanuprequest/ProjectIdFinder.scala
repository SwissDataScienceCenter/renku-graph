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

package io.renku.eventlog.events.consumers.cleanuprequest

import cats.effect.Async
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.model.projects

private trait ProjectIdFinder[F[_]] {
  def findProjectId(projectSlug: projects.Slug): F[Option[projects.GitLabId]]
}

private object ProjectIdFinder {
  def apply[F[_]: Async: SessionResource: QueriesExecutionTimes]: F[ProjectIdFinder[F]] =
    new ProjectIdFinderImpl[F].pure[F].widen
}

private class ProjectIdFinderImpl[F[_]: Async: SessionResource: QueriesExecutionTimes]
    extends DbClient(Some(QueriesExecutionTimes[F]))
    with ProjectIdFinder[F]
    with TypeSerializers {
  import skunk.implicits._

  override def findProjectId(projectSlug: projects.Slug): F[Option[projects.GitLabId]] = SessionResource[F].useK {
    measureExecutionTime {
      SqlStatement
        .named(s"${categoryName.show.toLowerCase} - find project_id")
        .select[projects.Slug, projects.GitLabId](sql"""
          SELECT project_id
          FROM project
          WHERE project_slug = $projectSlugEncoder
        """.query(projectIdDecoder))
        .arguments(projectSlug)
        .build(_.option)
    }
  }
}
