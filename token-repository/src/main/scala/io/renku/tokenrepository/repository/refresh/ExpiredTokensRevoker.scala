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

package io.renku.tokenrepository.repository.refresh

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}

private trait ExpiredTokensRevoker[F[_]] {
  def revokeToken(projectId: projects.Id, tokenId: AccessTokenId, accessToken: AccessToken): F[Unit]
}

private object ExpiredTokensRevoker {
  def apply[F[_]: Async: GitLabClient] = new ExpiredTokensRevokerImpl[F]
}

private class ExpiredTokensRevokerImpl[F[_]: Async: GitLabClient] extends ExpiredTokensRevoker[F] {

  import eu.timepit.refined.auto._
  import io.renku.http.tinytypes.TinyTypeURIEncoder._
  import org.http4s.Status.{Forbidden, NoContent, NotFound, Ok, Unauthorized}
  import org.http4s.implicits._
  import org.http4s.{Request, Response, Status}

  override def revokeToken(projectId: projects.Id, tokenId: AccessTokenId, accessToken: AccessToken): F[Unit] =
    GitLabClient[F]
      .delete(uri"projects" / projectId / "access_tokens" / tokenId, "revoke-project-access-token")(mapResponse)(
        accessToken.some
      )

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Unit]] = {
    case (Ok | NoContent | Unauthorized | Forbidden | NotFound, _, _) => ().pure[F]
  }
}
