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

package ch.datascience.tokenrepository.repository.fetching

import cats.effect.{ContextShift, Effect, IO}
import cats.implicits._
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.db.DbTransactor
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ApplicationLogger
import ch.datascience.tokenrepository.repository.ProjectsTokensDB
import io.chrisdavenport.log4cats.Logger
import io.circe.syntax._
import org.http4s.Response
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

import scala.language.higherKinds
import scala.util.control.NonFatal

class FetchTokenEndpoint[Interpretation[_]: Effect](
    tokenFinder: TokenFinder[Interpretation],
    logger:      Logger[Interpretation]
) extends Http4sDsl[Interpretation] {

  def fetchToken(projectId: ProjectId): Interpretation[Response[Interpretation]] =
    tokenFinder
      .findToken(projectId)
      .value
      .flatMap(toHttpResult(projectId))
      .recoverWith(httpResult(projectId))

  private def toHttpResult(projectId: ProjectId): Option[AccessToken] => Interpretation[Response[Interpretation]] = {
    case Some(token) =>
      Ok(token.asJson)
    case None =>
      NotFound(InfoMessage(s"Token for projectId: $projectId not found"))
  }

  private def httpResult(projectId: ProjectId): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage(s"Finding token for projectId: $projectId failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }
}

class IOFetchTokenEndpoint(
    transactor:          DbTransactor[IO, ProjectsTokensDB]
)(implicit contextShift: ContextShift[IO])
    extends FetchTokenEndpoint[IO](
      new IOTokenFinder(transactor),
      ApplicationLogger
    )
