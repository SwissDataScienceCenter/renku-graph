/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.users.projects

import Endpoint._
import cats.effect.Async
import cats.syntax.all._
import finder._
import io.renku.config.renku
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.{RenkuUrl, persons}
import io.renku.http.rest.paging.{PagingHeaders, PagingResponse}
import io.renku.http.server.security.model.AuthUser
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait Endpoint[F[_]] {
  def `GET /users/:id/projects`(criteria: Criteria, request: Request[F]): F[Response[F]]
}

object Endpoint {

  final case class Criteria(userId: persons.GitLabId, maybeUser: Option[AuthUser] = None)

  def apply[F[_]: Async: Logger]: F[Endpoint[F]] = for {
    projectsFinder <- ProjectsFinder[F]
    renkuUrl       <- RenkuUrlLoader()
    renkuApiUrl    <- renku.ApiUrl()
  } yield new EndpointImpl[F](projectsFinder, renkuUrl, renkuApiUrl)
}

private class EndpointImpl[F[_]: Async: Logger](projectsFinder: ProjectsFinder[F],
                                                renkuUrl:    RenkuUrl,
                                                renkuApiUrl: renku.ApiUrl
) extends Http4sDsl[F]
    with Endpoint[F] {

  import io.circe.Json
  import io.circe.syntax._
  import io.renku.http.ErrorMessage
  import io.renku.http.ErrorMessage._
  import org.http4s.circe.jsonEncoderOf
  import org.http4s.{EntityEncoder, Header, Request, Response, Status}

  private implicit val rnkUrl:    RenkuUrl     = renkuUrl
  private implicit val rnkApiUrl: renku.ApiUrl = renkuApiUrl

  override def `GET /users/:id/projects`(criteria: Criteria, request: Request[F]): F[Response[F]] =
    projectsFinder.findProjects(criteria) map toHttpResponse(request) recoverWith httpResult

  private def toHttpResponse(request: Request[F])(response: PagingResponse[model.Project]): Response[F] = {
    val resourceUrl = renku.ResourceUrl(show"$renkuUrl${request.uri}")
    Response[F](Status.Ok)
      .withEntity(response.results.asJson)
      .putHeaders(PagingHeaders.from(response)(resourceUrl, renku.ResourceUrl).toSeq.map(Header.ToRaw.rawToRaw): _*)
  }

  private implicit lazy val responseEntityEncoder: EntityEncoder[F, Json] = jsonEncoderOf[F, Json]

  private lazy val httpResult: PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage("Finding user's projects failed")
    Logger[F].error(exception)(errorMessage.value) >> InternalServerError(errorMessage)
  }
}
