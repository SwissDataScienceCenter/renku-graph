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

package io.renku.webhookservice.hookcreation

import cats.MonadThrow
import cats.effect._
import cats.syntax.all._
import io.renku.graph.model.projects.Id
import io.renku.http.ErrorMessage._
import io.renku.http.client.GitLabClient
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.server.security.model.AuthUser
import io.renku.http.{ErrorMessage, InfoMessage}
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
  def createHook(projectId: Id, authUser: AuthUser): F[Response[F]]
}

class HookCreationEndpointImpl[F[_]: MonadThrow: Logger](
    hookCreator: HookCreator[F]
) extends Http4sDsl[F]
    with HookCreationEndpoint[F] {

  def createHook(projectId: Id, authUser: AuthUser): F[Response[F]] = {
    for {
      creationResult <- hookCreator.createHook(projectId, authUser.accessToken)
      response       <- toHttpResponse(creationResult)
    } yield response
  } recoverWith httpResponse

  private lazy val toHttpResponse: CreationResult => F[Response[F]] = {
    case HookCreated => Created(InfoMessage("Hook created"))
    case HookExisted => Ok(InfoMessage("Hook already existed"))
  }

  private lazy val httpResponse: PartialFunction[Throwable, F[Response[F]]] = {
    case ex @ UnauthorizedException =>
      Response[F](Status.Unauthorized)
        .withEntity[ErrorMessage](ErrorMessage(ex))
        .pure[F]
    case NonFatal(exception) =>
      Logger[F]
        .error(exception)(exception.getMessage)
        .flatMap(_ => InternalServerError(ErrorMessage(exception)))
  }
}

object HookCreationEndpoint {
  def apply[F[_]: Async: GitLabClient: Logger: MetricsRegistry](projectHookUrl: ProjectHookUrl,
                                                                hookTokenCrypto: HookTokenCrypto[F]
  ): F[HookCreationEndpoint[F]] = for {
    hookCreator <- hookcreation.HookCreator(projectHookUrl, hookTokenCrypto)
  } yield new HookCreationEndpointImpl[F](hookCreator)
}
