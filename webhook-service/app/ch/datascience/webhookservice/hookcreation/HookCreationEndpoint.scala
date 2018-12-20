/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.graph.events.ProjectId
import ch.datascience.webhookservice.hookcreation.GitLabHookCreation.UnauthorizedException
import ch.datascience.webhookservice.model.GitLabAuthToken
import javax.inject.{ Inject, Singleton }
import play.api.mvc.{ AbstractController, ControllerComponents, Result }

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

@Singleton
class HookCreationEndpoint @Inject() (
    cc:          ControllerComponents,
    hookCreator: IOHookCreation
) extends AbstractController( cc ) {

  private implicit val executionContext: ExecutionContext = defaultExecutionContext

  import cats.effect._

  private implicit val cs: ContextShift[IO] = IO.contextShift( executionContext )

  def createHook( projectId: ProjectId ) = Action.async( parse.json[GitLabAuthToken] ) { implicit request =>
    hookCreator
      .createHook( projectId, request.body )
      .map( _ => Created )
      .unsafeToFuture()
      .recover( toResult )
  }

  private val toResult: PartialFunction[Throwable, Result] = {
    case UnauthorizedException => Unauthorized( ErrorMessage( "Unauthorized" ).toJson )
    case NonFatal( exception ) => BadGateway( ErrorMessage( exception.getMessage ).toJson )
  }
}
