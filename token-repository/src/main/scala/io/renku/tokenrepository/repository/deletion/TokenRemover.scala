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

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.projects.GitLabId
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.tokenrepository.repository.ProjectsTokensDB.SessionResource
import io.renku.tokenrepository.repository.metrics.QueriesExecutionTimes
import org.typelevel.log4cats.Logger

private[repository] trait TokenRemover[F[_]] {
  def delete(projectId: GitLabId, maybeAccessToken: Option[AccessToken]): F[DeletionResult]
}

private[repository] object TokenRemover {
  def apply[F[_]: Async: GitLabClient: SessionResource: Logger: QueriesExecutionTimes]: F[TokenRemover[F]] =
    TokensRevoker[F].map(new TokenRemoverImpl[F](PersistedTokenRemover[F], _))
}

private class TokenRemoverImpl[F[_]: MonadThrow](dbTokenRemover: PersistedTokenRemover[F],
                                                 tokensRevoker: TokensRevoker[F]
) extends TokenRemover[F] {

  import tokensRevoker._

  override def delete(projectId: GitLabId, maybeAccessToken: Option[AccessToken]): F[DeletionResult] =
    dbTokenRemover.delete(projectId).flatTap(_ => revokeTokens(projectId, maybeAccessToken))

  private def revokeTokens(projectId: GitLabId, maybeAccessToken: Option[AccessToken]) =
    maybeAccessToken match {
      case None              => ().pure[F]
      case Some(accessToken) => revokeAllTokens(projectId, except = None, accessToken)
    }
}
