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

package io.renku.tokenrepository.repository.deletion

import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.graph.model.projects.GitLabId
import io.renku.tokenrepository.repository.ProjectsTokensDB.SessionResource
import io.renku.tokenrepository.repository.TokenRepositoryTypeSerializers
import io.renku.tokenrepository.repository.metrics.QueriesExecutionTimes
import skunk.data.Completion
import skunk.implicits._

private[repository] trait PersistedTokenRemover[F[_]] {
  def delete(projectId: GitLabId): F[DeletionResult]
}

private[repository] object PersistedTokenRemover {
  def apply[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes]: PersistedTokenRemover[F] =
    new PersistedTokenRemoverImpl[F]
}

private class PersistedTokenRemoverImpl[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes]
    extends DbClient[F](Some(QueriesExecutionTimes[F]))
    with PersistedTokenRemover[F]
    with TokenRepositoryTypeSerializers {

  override def delete(projectId: GitLabId): F[DeletionResult] =
    SessionResource[F].useK {
      query(projectId)
    }

  private def query(projectId: GitLabId) = measureExecutionTime {
    SqlStatement
      .named("remove token")
      .command[GitLabId](sql"""DELETE FROM projects_tokens
                         WHERE project_id = $projectIdEncoder""".command)
      .arguments(projectId)
      .build
      .flatMapResult(failIfMultiUpdate(projectId))
  }

  private def failIfMultiUpdate(projectId: GitLabId): Completion => F[DeletionResult] = {
    case Completion.Delete(0) => DeletionResult.NotExisted.pure[F].widen
    case Completion.Delete(1) => DeletionResult.Deleted.pure[F].widen
    case Completion.Delete(n) =>
      new RuntimeException(s"Deleting token for a projectId: $projectId removed $n records")
        .raiseError[F, DeletionResult]
    case completion =>
      new RuntimeException(s"Deleting token for a projectId: $projectId failed with completion code $completion")
        .raiseError[F, DeletionResult]
  }
}
