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

package ch.datascience.webhookservice.hookvalidation

import cats.effect._
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.graph.model.projects.Id
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult._
import ch.datascience.webhookservice.project.ProjectHookUrl
import ch.datascience.webhookservice.security.AccessTokenExtractor
import io.chrisdavenport.log4cats.Logger
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response, Status}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class HookValidationEndpoint[Interpretation[_]: Effect](
    hookValidator:     HookValidator[Interpretation],
    accessTokenFinder: AccessTokenExtractor[Interpretation],
    logger:            Logger[Interpretation]
) extends Http4sDsl[Interpretation] {

  import accessTokenFinder._

  def validateHook(projectId: Id, request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      accessToken    <- findAccessToken(request)
      creationResult <- hookValidator.validateHook(projectId, Some(accessToken))
      response       <- toHttpResponse(creationResult)
    } yield response
  } recoverWith withHttpResult

  private lazy val toHttpResponse: HookValidationResult => Interpretation[Response[Interpretation]] = {
    case HookExists  => Ok(InfoMessage("Hook valid"))
    case HookMissing => NotFound(InfoMessage("Hook not found"))
  }

  private lazy val withHttpResult: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case ex @ UnauthorizedException =>
      Response[Interpretation](Status.Unauthorized)
        .withEntity[ErrorMessage](ErrorMessage(ex))
        .pure[Interpretation]
    case NonFatal(exception) =>
      logger.error(exception)(exception.getMessage)
      InternalServerError(ErrorMessage(exception))
  }
}

object IOHookValidationEndpoint {
  def apply(
      projectHookUrl:  ProjectHookUrl,
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[HookValidationEndpoint[IO]] =
    for {
      hookValidator <- IOHookValidator(projectHookUrl, gitLabThrottler)
    } yield new HookValidationEndpoint[IO](
      hookValidator,
      new AccessTokenExtractor[IO],
      logger
    )
}
