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

import cats.MonadThrow
import cats.effect._
import cats.syntax.all._
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.model.projects.Id
import io.renku.http.ErrorMessage._
import io.renku.http.client.GitLabClient
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.server.security.model.AuthUser
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.webhookservice.hookdeletion.HookDeletor.DeletionResult
import io.renku.webhookservice.hookdeletion.HookDeletor.DeletionResult.{HookDeleted, HookNotFound}
import io.renku.webhookservice.model.{HookIdentifier, ProjectHookUrl}
import org.http4s.dsl.Http4sDsl
import org.http4s.{Response, Status}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait HookDeletionEndpoint[F[_]] {
  def deleteHook(projectId: Id, authUser: AuthUser): F[Response[F]]
}

class HookDeletionEndpointImpl[F[_]: MonadThrow: Logger](
    projectHookUrl: ProjectHookUrl,
    hookDeletor:    HookDeletor[F]
) extends Http4sDsl[F]
    with HookDeletionEndpoint[F] {

  private lazy val toHttpResponse: DeletionResult => F[Response[F]] = {
    case HookNotFound => NotFound(InfoMessage("Hook already deleted"))
    case HookDeleted  => Ok(InfoMessage("Hook deleted"))
  }
  private lazy val httpResponse: PartialFunction[Throwable, F[Response[F]]] = {
    case ex @ UnauthorizedException =>
      Response[F](Status.Unauthorized)
        .withEntity[ErrorMessage](ErrorMessage(ex))
        .pure[F]
    case NonFatal(exception) =>
      Logger[F].error(exception)(exception.getMessage) >> InternalServerError(ErrorMessage(exception))
  }

  def deleteHook(projectId: Id, authUser: AuthUser): F[Response[F]] = {
    for {
      deletionResult <- hookDeletor.deleteHook(HookIdentifier(projectId, projectHookUrl), authUser.accessToken)
      response       <- toHttpResponse(deletionResult)
    } yield response
  } recoverWith httpResponse
}

object HookDeletionEndpoint {
  def apply[F[_]: Async: Logger](
      projectHookUrl:  ProjectHookUrl,
      gitLabThrottler: Throttler[F, GitLab],
      gitLabClient:    GitLabClient[F]
  ): F[HookDeletionEndpoint[F]] = for {
    hookDeletor <- HookDeletor(gitLabThrottler, gitLabClient)
  } yield new HookDeletionEndpointImpl[F](projectHookUrl, hookDeletor)
}
