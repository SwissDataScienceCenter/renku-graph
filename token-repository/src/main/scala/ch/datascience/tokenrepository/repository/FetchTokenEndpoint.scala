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

package ch.datascience.tokenrepository.repository

import cats.effect.{Effect, IO}
import cats.implicits._
import ch.datascience.clients.AccessToken
import ch.datascience.clients.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.graph.events.ProjectId
import ch.datascience.tokenrepository.ApplicationLogger
import io.chrisdavenport.log4cats.Logger
import io.circe.Json
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Response}

import scala.language.higherKinds
import scala.util.control.NonFatal

class FetchTokenEndpoint[F[_]: Effect](
    tokensRepository: TokensRepository[F],
    logger:           Logger[F]
) extends Http4sDsl[F] {

  val fetchToken: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "projects" / ProjectIdPathBinder(projectId) / "tokens" =>
      tokensRepository
        .findToken(projectId)
        .value
        .flatMap(toHttpResult(projectId))
        .recoverWith(withHttpResult(projectId))
  }

  private def toHttpResult(projectId: ProjectId): Option[AccessToken] => F[Response[F]] = {
    case Some(token) =>
      logger.info(s"Token for projectId: $projectId found")
      Ok(toJson(token))
    case None =>
      val message = InfoMessage(s"Token for projectId: $projectId not found")
      logger.info(message.value)
      NotFound(message)
  }

  private def toJson: AccessToken => Json = {
    case PersonalAccessToken(token) => Json.obj("personalAccessToken" -> Json.fromString(token))
    case OAuthAccessToken(token)    => Json.obj("oauthAccessToken"    -> Json.fromString(token))
  }

  private def withHttpResult(projectId: ProjectId): PartialFunction[Throwable, F[Response[F]]] = {
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage(s"Finding token for projectId: $projectId failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }
}

class IOFetchTokenEndpoint
    extends FetchTokenEndpoint[IO](
      new IOTokensRepository(),
      ApplicationLogger
    )
