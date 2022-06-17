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

package io.renku.webhookservice.hookdeletion

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Id
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import io.renku.webhookservice.hookdeletion.HookDeletor.DeletionResult
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher.HookIdAndUrl
import io.renku.webhookservice.model.ProjectHookUrl
import org.http4s.Status
import org.http4s.Status.{NotFound, Ok}
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.log4cats.Logger

private trait ProjectHookDeletor[F[_]] {
  def delete(
      projectId:     projects.Id,
      projectHookId: HookIdAndUrl,
      accessToken:   AccessToken
  ): F[DeletionResult]
}

private class ProjectHookDeletorImpl[F[_]: Async: GitLabClient: Logger] extends ProjectHookDeletor[F] {

  import cats.effect._
  import io.renku.http.client.RestClientError.UnauthorizedException
  import org.http4s.Status.Unauthorized
  import org.http4s.{Request, Response}

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[DeletionResult]] = {
    case (Ok, _, _)           => DeletionResult.HookDeleted.pure[F].widen[DeletionResult]
    case (NotFound, _, _)     => DeletionResult.HookNotFound.pure[F].widen[DeletionResult]
    case (Unauthorized, _, _) => MonadCancelThrow[F].raiseError(UnauthorizedException)
  }

  def delete(projectId: projects.Id, projectHookId: HookIdAndUrl, accessToken: AccessToken): F[DeletionResult] =
    GitLabClient[F].delete(uri"projects" / projectId.show / "hooks" / projectHookId.id.show, "delete-hook")(
      mapResponse
    )(
      accessToken.some
    )
}

private object ProjectHookDeletor {
  def apply[F[_]: Async: GitLabClient: Logger]: F[ProjectHookDeletor[F]] =
    new ProjectHookDeletorImpl[F].pure[F].widen[ProjectHookDeletor[F]]

  final case class ProjectHook(
      projectId:           Id,
      projectHookUrl:      ProjectHookUrl,
      serializedHookToken: SerializedHookToken
  )
}
