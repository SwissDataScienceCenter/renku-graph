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

package ch.datascience.webhookservice.hookcreation

import cats.effect.IO
import cats.implicits._
import ch.datascience.clients.AccessToken
import ch.datascience.clients.AccessToken._
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.graph.events.ProjectId
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.hookcreation.HookCreator.HookCreationResult
import ch.datascience.webhookservice.hookcreation.HookCreator.HookCreationResult.{HookCreated, HookExisted}
import javax.inject.{Inject, Singleton}
import play.api.mvc._

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

@Singleton
class HookCreationEndpoint @Inject()(
    cc:          ControllerComponents,
    hookCreator: IOHookCreator
) extends AbstractController(cc) {

  private implicit val executionContext: ExecutionContext = defaultExecutionContext

  def createHook(projectId: ProjectId): Action[AnyContent] = Action.async { implicit request =>
    (for {
      accessToken    <- findAccessToken(request)
      creationResult <- hookCreator.createHook(projectId, accessToken)
    } yield toHttpResult(creationResult))
      .unsafeToFuture()
      .recover(withHttpResult)
  }

  private def findAccessToken(request: Request[_]): IO[AccessToken] = IO.fromEither {
    convert(request.headers.get("OAUTH-TOKEN"), to = OAuthAccessToken.from)
      .orElse(convert(request.headers.get("PRIVATE-TOKEN"), to = PersonalAccessToken.from))
      .getOrElse(Left(UnauthorizedException))
  }

  private def convert(headers: Option[String], to: String => Either[Exception, AccessToken]) =
    headers.map {
      to(_).leftMap(_ => UnauthorizedException)
    }

  private lazy val toHttpResult: HookCreationResult => Result = {
    case HookCreated => Created
    case HookExisted => Ok
  }

  private val withHttpResult: PartialFunction[Throwable, Result] = {
    case ex @ UnauthorizedException => Unauthorized(ErrorMessage(ex.getMessage).toJson)
    case NonFatal(exception)        => InternalServerError(ErrorMessage(exception.getMessage).toJson)
  }
}
