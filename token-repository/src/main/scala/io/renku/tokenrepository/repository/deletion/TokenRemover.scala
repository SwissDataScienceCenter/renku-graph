/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import org.typelevel.log4cats.Logger
import skunk.data.Completion
import skunk.implicits._

private[repository] trait TokenRemover[F[_]] {
  def delete(projectId: GitLabId): F[Unit]
}

private[repository] object TokenRemover {
  def apply[F[_]: MonadCancelThrow: SessionResource: Logger: QueriesExecutionTimes]: TokenRemover[F] =
    new TokenRemoverImpl[F]
}

private class TokenRemoverImpl[F[_]: MonadCancelThrow: SessionResource: Logger: QueriesExecutionTimes]
    extends DbClient[F](Some(QueriesExecutionTimes[F]))
    with TokenRemover[F]
    with TokenRepositoryTypeSerializers {

  override def delete(projectId: GitLabId): F[Unit] =
    SessionResource[F].useK {
      measureExecutionTime(query(projectId))
    } >> Logger[F].info(show"token removed for $projectId")

  private def query(projectId: GitLabId) =
    SqlStatement
      .named("remove token")
      .command[GitLabId](sql"""DELETE FROM projects_tokens
                         WHERE project_id = $projectIdEncoder""".command)
      .arguments(projectId)
      .build
      .flatMapResult(failIfMultiUpdate(projectId))

  private def failIfMultiUpdate(projectId: GitLabId): Completion => F[Unit] = {
    case Completion.Delete(0 | 1) => ().pure[F]
    case Completion.Delete(n) =>
      new RuntimeException(s"Deleting token for a projectId: $projectId removed $n records")
        .raiseError[F, Unit]
    case completion =>
      new RuntimeException(s"Deleting token for a projectId: $projectId failed with completion code $completion")
        .raiseError[F, Unit]
  }
}
