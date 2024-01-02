/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.repository.fetching

import cats.MonadThrow
import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import io.circe.syntax._
import io.renku.data.Message
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.tokenrepository.repository.ProjectsTokensDB.SessionResource
import io.renku.tokenrepository.repository.metrics.QueriesExecutionTimes
import org.http4s.Response
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait FetchTokenEndpoint[F[_]] {
  def fetchToken[ID](
      projectIdentifier: ID
  )(implicit findToken: ID => OptionT[F, AccessToken]): F[Response[F]]

  implicit val findById:   projects.GitLabId => OptionT[F, AccessToken]
  implicit val findBySlug: projects.Slug => OptionT[F, AccessToken]
}

class FetchTokenEndpointImpl[F[_]: MonadThrow: Logger](tokenFinder: TokenFinder[F])
    extends Http4sDsl[F]
    with FetchTokenEndpoint[F] {

  override def fetchToken[ID](
      projectIdentifier: ID
  )(implicit findToken: ID => OptionT[F, AccessToken]): F[Response[F]] =
    findToken(projectIdentifier).value
      .flatMap(toHttpResult(projectIdentifier))
      .recoverWith(httpResult(projectIdentifier))

  private def toHttpResult[ID](
      projectIdentifier: ID
  ): Option[AccessToken] => F[Response[F]] = {
    case Some(token) => Ok(token.asJson)
    case None        => NotFound(Message.Info.unsafeApply(s"Token for project: $projectIdentifier not found"))
  }

  private def httpResult[ID](
      projectIdentifier: ID
  ): PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = Message.Error.unsafeApply(s"Finding token for project: $projectIdentifier failed")
    Logger[F].error(exception)(errorMessage.show) *> InternalServerError(errorMessage)
  }

  implicit val findById:   projects.GitLabId => OptionT[F, AccessToken] = tokenFinder.findToken
  implicit val findBySlug: projects.Slug => OptionT[F, AccessToken]     = tokenFinder.findToken
}

object FetchTokenEndpoint {
  def apply[F[_]: Async: Logger: SessionResource: QueriesExecutionTimes]: F[FetchTokenEndpoint[F]] =
    TokenFinder[F].map(new FetchTokenEndpointImpl[F](_))
}
