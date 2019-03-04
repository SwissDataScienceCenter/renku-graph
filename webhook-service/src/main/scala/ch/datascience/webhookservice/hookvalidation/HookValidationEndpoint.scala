/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import cats.effect.{ContextShift, Effect, IO}
import cats.implicits._
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult._
import ch.datascience.webhookservice.security.AccessTokenExtractor
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response, Status}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class HookValidationEndpoint[Interpretation[_]: Effect](
    hookValidator:     HookValidator[Interpretation],
    accessTokenFinder: AccessTokenExtractor[Interpretation]
) extends Http4sDsl[Interpretation] {

  import accessTokenFinder._

  def validateHook(projectId: ProjectId, request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      accessToken    <- findAccessToken(request)
      creationResult <- hookValidator.validateHook(projectId, accessToken)
      response       <- toHttpResponse(creationResult)
    } yield response
  } recoverWith withHttpResult

  private lazy val toHttpResponse: HookValidationResult => Interpretation[Response[Interpretation]] = {
    case HookExists  => Ok()
    case HookMissing => NotFound()
  }

  private lazy val withHttpResult: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case ex @ UnauthorizedException =>
      Response[Interpretation](Status.Unauthorized)
        .withEntity[ErrorMessage](ErrorMessage(ex.getMessage))
        .pure[Interpretation]
    case NonFatal(exception) => InternalServerError(ErrorMessage(exception.getMessage))
  }
}

class IOHookValidationEndpoint(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO])
    extends HookValidationEndpoint[IO](
      new IOHookValidator,
      new AccessTokenExtractor[IO]
    )
