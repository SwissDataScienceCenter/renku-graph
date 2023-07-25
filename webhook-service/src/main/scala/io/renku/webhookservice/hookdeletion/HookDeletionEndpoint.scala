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

package io.renku.webhookservice.hookdeletion

import cats.MonadThrow
import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.data.Message
import io.renku.graph.model.projects.GitLabId
import io.renku.http.client.GitLabClient
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.server.security.model.AuthUser
import io.renku.webhookservice.hookdeletion.HookRemover.DeletionResult
import io.renku.webhookservice.hookdeletion.HookRemover.DeletionResult.HookDeleted
import io.renku.webhookservice.model.{HookIdentifier, ProjectHookUrl}
import org.http4s.dsl.Http4sDsl
import org.http4s.{Response, Status}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait HookDeletionEndpoint[F[_]] {
  def deleteHook(projectId: GitLabId, authUser: AuthUser): F[Response[F]]
}

class HookDeletionEndpointImpl[F[_]: MonadThrow: Logger](
    projectHookUrl: ProjectHookUrl,
    hookDeletor:    HookRemover[F]
) extends Http4sDsl[F]
    with HookDeletionEndpoint[F] {

  def deleteHook(projectId: GitLabId, authUser: AuthUser): F[Response[F]] = {
    hookDeletor.deleteHook(HookIdentifier(projectId, projectHookUrl), authUser.accessToken).flatMap(toHttpResponse(_))
  } recoverWith httpResponse

  private lazy val toHttpResponse: Option[DeletionResult] => F[Response[F]] = {
    case Some(HookDeleted) => Ok(Message.Info("Hook deleted"))
    case _                 => NotFound(Message.Info("Hook not found"))
  }
  private lazy val httpResponse: PartialFunction[Throwable, F[Response[F]]] = {
    case ex @ UnauthorizedException =>
      Response[F](Status.Unauthorized)
        .withEntity(Message.Error.fromExceptionMessage(ex))
        .pure[F]
    case NonFatal(exception) =>
      Logger[F].error(exception)(exception.getMessage) >>
        InternalServerError(Message.Error.fromExceptionMessage(exception))
  }
}

object HookDeletionEndpoint {
  def apply[F[_]: Async: GitLabClient: Logger](projectHookUrl: ProjectHookUrl): F[HookDeletionEndpoint[F]] = for {
    hookDeletor <- HookRemover[F]
  } yield new HookDeletionEndpointImpl[F](projectHookUrl, hookDeletor)
}
