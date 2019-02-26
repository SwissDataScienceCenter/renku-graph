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

import cats.data.NonEmptyList
import cats.effect.Effect
import cats.implicits._
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.graph.http.server.ProjectIdPathBinder
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.hookcreation.HookCreator.HookCreationResult
import ch.datascience.webhookservice.hookcreation.HookCreator.HookCreationResult.{HookCreated, HookExisted}
import ch.datascience.webhookservice.security.AccessTokenFinder
import org.http4s.AuthScheme.Basic
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`WWW-Authenticate`
import org.http4s.{Challenge, HttpRoutes, Response}

import scala.language.higherKinds
import scala.util.control.NonFatal

class HookCreationEndpoint[Interpretation[_]: Effect](
    hookCreator:       HookCreator[Interpretation],
    accessTokenFinder: AccessTokenFinder[Interpretation]
) extends Http4sDsl[Interpretation] {

  import accessTokenFinder._

  val createHook: HttpRoutes[Interpretation] = HttpRoutes.of[Interpretation] {
    case request @ POST -> Root / "projects" / ProjectIdPathBinder(projectId) / "webhooks" => {
      for {
        accessToken    <- findAccessToken(request)
        creationResult <- hookCreator.createHook(projectId, accessToken)
        response       <- toHttpResult(creationResult)
      } yield response
    } recoverWith withHttpResult
  }

  private lazy val toHttpResult: HookCreationResult => Interpretation[Response[Interpretation]] = {
    case HookCreated => Created()
    case HookExisted => Ok()
  }

  private lazy val withHttpResult: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case ex @ UnauthorizedException =>
      Unauthorized(
        `WWW-Authenticate`(
          NonEmptyList.of(
            Challenge(scheme = Basic.value, realm = "Please provide valid 'OAUTH-TOKEN' or 'PRIVATE-TOKEN'"))
        ),
        ErrorMessage(ex.getMessage)
      )
    case NonFatal(exception) => InternalServerError(ErrorMessage(exception.getMessage))
  }
}
