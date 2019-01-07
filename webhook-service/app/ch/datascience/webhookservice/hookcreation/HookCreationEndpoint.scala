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
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.graph.events.ProjectId
import ch.datascience.webhookservice.hookcreation.HookCreationRequestSender.UnauthorizedException
import ch.datascience.webhookservice.model.UserAuthToken
import javax.inject.{ Inject, Singleton }
import play.api.mvc.{ AbstractController, ControllerComponents, Request, Result }

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

@Singleton
class HookCreationEndpoint @Inject() (
    cc:          ControllerComponents,
    hookCreator: IOHookCreator
) extends AbstractController( cc ) {

  private implicit val executionContext: ExecutionContext = defaultExecutionContext

  def createHook( projectId: ProjectId ) = Action.async { implicit request =>
    ( for {
      userAuthToken <- findUserAuthToken( request )
      createdResult <- hookCreator
        .createHook( projectId, userAuthToken )
        .map( _ => Created )
    } yield createdResult )
      .unsafeToFuture()
      .recover( toResult )
  }

  private def findUserAuthToken( request: Request[_] ): IO[UserAuthToken] = IO.fromEither {
    request.headers.get( "PRIVATE-TOKEN" ) match {
      case None             => Left( UnauthorizedException )
      case Some( rawToken ) => UserAuthToken.from( rawToken ).leftMap( _ => UnauthorizedException )
    }
  }

  private val toResult: PartialFunction[Throwable, Result] = {
    case UnauthorizedException => Unauthorized( ErrorMessage( "Unauthorized" ).toJson )
    case NonFatal( exception ) => BadGateway( ErrorMessage( exception.getMessage ).toJson )
  }
}
