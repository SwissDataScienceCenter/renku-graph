/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.hookcreation

import cats.effect._
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.graph.model.projects.Id
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.webhookservice.crypto.HookTokenCrypto
import ch.datascience.webhookservice.hookcreation.HookCreator.CreationResult
import ch.datascience.webhookservice.hookcreation.HookCreator.CreationResult.{HookCreated, HookExisted}
import ch.datascience.webhookservice.project.ProjectHookUrl
import ch.datascience.webhookservice.security.AccessTokenExtractor
import io.chrisdavenport.log4cats.Logger
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response, Status}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class HookCreationEndpoint[Interpretation[_]: Effect](
    hookCreator:       HookCreator[Interpretation],
    accessTokenFinder: AccessTokenExtractor[Interpretation]
) extends Http4sDsl[Interpretation] {

  import accessTokenFinder._

  def createHook(projectId: Id, request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      accessToken    <- findAccessToken(request)
      creationResult <- hookCreator.createHook(projectId, accessToken)
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
    case NonFatal(exception) => InternalServerError(ErrorMessage(exception))
  }
}

object IOHookCreationEndpoint {
  def apply(
      projectHookUrl:        ProjectHookUrl,
      gitLabThrottler:       Throttler[IO, GitLab],
      hookTokenCrypto:       HookTokenCrypto[IO],
      executionTimeRecorder: ExecutionTimeRecorder[IO],
      logger:                Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      clock:            Clock[IO],
      timer:            Timer[IO]
  ): IO[HookCreationEndpoint[IO]] =
    for {
      hookCreator <- IOHookCreator(projectHookUrl, gitLabThrottler, hookTokenCrypto, executionTimeRecorder, logger)
    } yield new HookCreationEndpoint[IO](hookCreator, new AccessTokenExtractor[IO])
}
