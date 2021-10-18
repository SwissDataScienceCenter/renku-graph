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

package io.renku.webhookservice.hookcreation

import HookCreator.CreationResult
import HookCreator.CreationResult.{HookCreated, HookExisted}
import cats.effect._
import cats.syntax.all._
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.model.projects.Id
import io.renku.http.ErrorMessage._
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.server.security.model.AuthUser
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.webhookservice.crypto.HookTokenCrypto
import io.renku.webhookservice.hookcreation
import io.renku.webhookservice.model.ProjectHookUrl
import org.http4s.dsl.Http4sDsl
import org.http4s.{Response, Status}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

trait HookCreationEndpoint[Interpretation[_]] {
  def createHook(projectId: Id, authUser: AuthUser): Interpretation[Response[Interpretation]]
}

class HookCreationEndpointImpl[Interpretation[_]: Effect](
    hookCreator: HookCreator[Interpretation],
    logger:      Logger[Interpretation]
) extends Http4sDsl[Interpretation]
    with HookCreationEndpoint[Interpretation] {

  def createHook(projectId: Id, authUser: AuthUser): Interpretation[Response[Interpretation]] = {
    for {
      creationResult <- hookCreator.createHook(projectId, authUser.accessToken)
      response       <- toHttpResponse(creationResult)
    } yield response
  } recoverWith httpResponse

  private lazy val toHttpResponse: CreationResult => Interpretation[Response[Interpretation]] = {
    case HookCreated => Created(InfoMessage("Hook created"))
    case HookExisted => Ok(InfoMessage("Hook already existed"))
  }

  private lazy val httpResponse: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case ex @ UnauthorizedException =>
      Response[Interpretation](Status.Unauthorized)
        .withEntity[ErrorMessage](ErrorMessage(ex))
        .pure[Interpretation]
    case NonFatal(exception) =>
      logger.error(exception)(exception.getMessage)
      InternalServerError(ErrorMessage(exception))
  }
}

object IOHookCreationEndpoint {
  def apply(
      projectHookUrl:  ProjectHookUrl,
      gitLabThrottler: Throttler[IO, GitLab],
      hookTokenCrypto: HookTokenCrypto[IO],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      clock:            Clock[IO],
      timer:            Timer[IO]
  ): IO[HookCreationEndpoint[IO]] = for {
    hookCreator <- hookcreation.HookCreator(projectHookUrl, gitLabThrottler, hookTokenCrypto, logger)
  } yield new HookCreationEndpointImpl[IO](hookCreator, logger)
}
