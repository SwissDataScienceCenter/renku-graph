/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.projects.Id
import io.renku.graph.model.{GitLabUrl, projects}
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import io.renku.webhookservice.hookdeletion.HookDeletor.DeletionResult
import io.renku.webhookservice.model.ProjectHookUrl
import org.http4s.Status
import org.http4s.Status.{NotFound, Ok}
import org.typelevel.log4cats.Logger

private trait ProjectHookDeletor[F[_]] {
  def delete(
      projectId:   projects.Id,
      accessToken: AccessToken
  ): F[DeletionResult]
}

private class ProjectHookDeletorImpl[F[_]: Async: Logger](
    gitLabUrl:       GitLabUrl,
    gitLabThrottler: Throttler[F, GitLab]
) extends RestClient(gitLabThrottler)
    with ProjectHookDeletor[F] {

  import cats.effect._
  import io.renku.http.client.RestClientError.UnauthorizedException
  import org.http4s.Method.DELETE
  import org.http4s.Status.Unauthorized
  import org.http4s.{Request, Response}

  def delete(projectId: projects.Id, accessToken: AccessToken): F[DeletionResult] =
    for {
      uri    <- validateUri(s"$gitLabUrl/api/v4/projects/$projectId/hooks")
      result <- send(request(DELETE, uri, accessToken))(mapResponse)
    } yield result

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[DeletionResult]] = {
    case (Ok, _, _)           => DeletionResult.HookDeleted.pure[F].widen[DeletionResult]
    case (NotFound, _, _)     => DeletionResult.HookNotFound.pure[F].widen[DeletionResult]
    case (Unauthorized, _, _) => MonadCancelThrow[F].raiseError(UnauthorizedException)
  }
}

private object ProjectHookDeletor {
  final case class ProjectHook(
      projectId:           Id,
      projectHookUrl:      ProjectHookUrl,
      serializedHookToken: SerializedHookToken
  )

  def apply[F[_]: Async: Logger](gitLabThrottler: Throttler[F, GitLab]): F[ProjectHookDeletor[F]] =
    for {
      gitLabUrl <- GitLabUrlLoader[F]()
    } yield new ProjectHookDeletorImpl(gitLabUrl, gitLabThrottler)
}
