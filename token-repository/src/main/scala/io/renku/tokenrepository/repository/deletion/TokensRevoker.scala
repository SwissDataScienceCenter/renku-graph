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

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.projects.GitLabId
import io.renku.http.client.{AccessToken, GitLabClient}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private[repository] trait TokensRevoker[F[_]] {
  def revokeAllTokens(projectId: GitLabId, accessToken: AccessToken): F[Unit]
}

private[repository] object TokensRevoker {
  def apply[F[_]: Async: GitLabClient: Logger]: F[TokensRevoker[F]] =
    RevokeCandidatesFinder[F].map(new TokensRevokerImpl[F](_, TokenRevoker[F]))
}

private class TokensRevokerImpl[F[_]: Async: Logger](revokeCandidatesFinder: RevokeCandidatesFinder[F],
                                                     tokenRevoker: TokenRevoker[F]
) extends TokensRevoker[F] {

  import revokeCandidatesFinder._
  import tokenRevoker._

  override def revokeAllTokens(projectId: GitLabId, accessToken: AccessToken): F[Unit] =
    projectAccessTokensStream(projectId, accessToken)
      .evalMap(revokeToken(_, projectId, accessToken))
      .compile
      .drain
      .recoverWith { case NonFatal(ex) =>
        Logger[F].warn(ex)(show"removing old token in GitLab for project $projectId failed")
      }
}
