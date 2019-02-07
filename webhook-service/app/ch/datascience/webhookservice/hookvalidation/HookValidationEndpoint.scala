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

import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.graph.events.ProjectId
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult._
import ch.datascience.webhookservice.security.IOAccessTokenFinder
import javax.inject.{Inject, Singleton}
import play.api.mvc._

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

@Singleton
class HookValidationEndpoint @Inject()(
    cc:                ControllerComponents,
    hookValidator:     IOHookValidator,
    accessTokenFinder: IOAccessTokenFinder
) extends AbstractController(cc) {

  import accessTokenFinder._

  private implicit val executionContext: ExecutionContext = defaultExecutionContext

  def validateHook(projectId: ProjectId): Action[AnyContent] = Action.async { implicit request =>
    (for {
      accessToken    <- findAccessToken(request)
      creationResult <- hookValidator.validateHook(projectId, accessToken)
    } yield toHttpResult(creationResult))
      .unsafeToFuture()
      .recover(withHttpResult)
  }

  private lazy val toHttpResult: HookValidationResult => Result = {
    case HookExists  => Ok
    case HookMissing => NotFound
  }

  private val withHttpResult: PartialFunction[Throwable, Result] = {
    case ex @ UnauthorizedException => Unauthorized(ErrorMessage(ex.getMessage).toJson)
    case NonFatal(exception)        => InternalServerError(ErrorMessage(exception.getMessage).toJson)
  }
}
