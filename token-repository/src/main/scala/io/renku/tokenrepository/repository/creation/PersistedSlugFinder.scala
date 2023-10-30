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

package io.renku.tokenrepository.repository.creation

import cats.effect.Async
import io.renku.db.DbClient
import io.renku.graph.model.projects
import io.renku.tokenrepository.repository.ProjectsTokensDB.SessionResource
import io.renku.tokenrepository.repository.TokenRepositoryTypeSerializers
import io.renku.tokenrepository.repository.metrics.QueriesExecutionTimes

private trait PersistedSlugFinder[F[_]] {
  def findPersistedProjectSlug(projectId: projects.GitLabId): F[Option[projects.Slug]]
}

private object PersistedSlugFinder {
  def apply[F[_]: Async: SessionResource: QueriesExecutionTimes]: PersistedSlugFinder[F] =
    new PersistedSlugFinderImpl[F]
}

private class PersistedSlugFinderImpl[F[_]: Async: SessionResource: QueriesExecutionTimes]
    extends DbClient[F](Some(QueriesExecutionTimes[F]))
    with PersistedSlugFinder[F]
    with TokenRepositoryTypeSerializers {

  import io.renku.db.SqlStatement
  import skunk.implicits._

  override def findPersistedProjectSlug(projectId: projects.GitLabId): F[Option[projects.Slug]] =
    SessionResource[F].useK(measureExecutionTime(query(projectId)))

  private def query(projectId: projects.GitLabId) =
    SqlStatement
      .named("find slug for token")
      .select[projects.GitLabId, projects.Slug](
        sql"""SELECT project_slug
              FROM projects_tokens
              WHERE project_id = $projectIdEncoder"""
          .query(projectSlugDecoder)
      )
      .arguments(projectId)
      .build(_.option)
}
