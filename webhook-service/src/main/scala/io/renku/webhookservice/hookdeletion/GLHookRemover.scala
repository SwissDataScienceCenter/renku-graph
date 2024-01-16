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

package io.renku.webhookservice.hookdeletion

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.webhookservice.hookdeletion.HookRemover.DeletionResult
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher.HookIdAndUrl
import org.http4s.Status
import org.http4s.Status.{Forbidden, NoContent, NotFound, Ok}
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.log4cats.Logger

private trait GLHookRemover[F[_]] {
  def delete(projectId: projects.GitLabId, projectHookId: HookIdAndUrl, accessToken: AccessToken): F[DeletionResult]
}

private class GLHookRemoverImpl[F[_]: Async: GitLabClient: Logger] extends GLHookRemover[F] {

  import cats.effect._
  import io.renku.http.client.RestClientError.UnauthorizedException
  import io.renku.http.tinytypes.TinyTypeURIEncoder._
  import org.http4s.Status.Unauthorized
  import org.http4s.{Request, Response}

  def delete(projectId: projects.GitLabId, projectHookId: HookIdAndUrl, accessToken: AccessToken): F[DeletionResult] =
    GitLabClient[F].delete(uri"projects" / projectId / "hooks" / projectHookId.id, "delete-hook")(
      mapResponse
    )(accessToken.some)

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[DeletionResult]] = {
    case (Ok | NoContent, _, _)           => DeletionResult.HookDeleted.pure[F].widen[DeletionResult]
    case (NotFound, _, _)                 => DeletionResult.HookNotFound.pure[F].widen[DeletionResult]
    case (Unauthorized | Forbidden, _, _) => MonadCancelThrow[F].raiseError(UnauthorizedException)
  }
}

private object GLHookRemover {
  def apply[F[_]: Async: GitLabClient: Logger]: F[GLHookRemover[F]] =
    new GLHookRemoverImpl[F].pure[F].widen[GLHookRemover[F]]
}
