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

package io.renku.webhookservice.hookcreation

import cats.MonadThrow
import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.data.Message
import io.renku.graph.model.projects.GitLabId
import io.renku.http.client.GitLabClient
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.server.security.model.AuthUser
import io.renku.metrics.MetricsRegistry
import io.renku.webhookservice.crypto.HookTokenCrypto
import io.renku.webhookservice.hookcreation
import io.renku.webhookservice.hookcreation.HookCreator.CreationResult
import io.renku.webhookservice.hookcreation.HookCreator.CreationResult.{HookCreated, HookExisted}
import io.renku.webhookservice.model.ProjectHookUrl
import org.http4s.dsl.Http4sDsl
import org.http4s.{Response, Status}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait HookCreationEndpoint[F[_]] {
  def createHook(projectId: GitLabId, authUser: AuthUser): F[Response[F]]
}

class HookCreationEndpointImpl[F[_]: MonadThrow: Logger](
    hookCreator: HookCreator[F]
) extends Http4sDsl[F]
    with HookCreationEndpoint[F] {

  def createHook(projectId: GitLabId, authUser: AuthUser): F[Response[F]] = {
    hookCreator.createHook(projectId, authUser) >>= toHttpResponse
  } recoverWith httpResponse

  private lazy val toHttpResponse: Option[CreationResult] => F[Response[F]] = {
    case Some(HookCreated) => Created(Message.Info("Hook created"))
    case Some(HookExisted) => Ok(Message.Info("Hook already existed"))
    case None              => NotFound(Message.Info("Project not found"))
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

object HookCreationEndpoint {
  def apply[F[_]: Async: GitLabClient: Logger: MetricsRegistry](projectHookUrl:  ProjectHookUrl,
                                                                hookTokenCrypto: HookTokenCrypto[F]
  ): F[HookCreationEndpoint[F]] = for {
    hookCreator <- hookcreation.HookCreator(projectHookUrl, hookTokenCrypto)
  } yield new HookCreationEndpointImpl[F](hookCreator)
}
