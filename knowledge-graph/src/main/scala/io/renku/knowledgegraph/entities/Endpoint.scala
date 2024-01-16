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

package io.renku.knowledgegraph.entities

import cats.NonEmptyParallel
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.syntax._
import io.renku.config.renku
import io.renku.config.renku.ResourceUrl
import io.renku.data.Message
import io.renku.entities.search.{Criteria, EntitiesFinder, model}
import io.renku.graph.config.{GitLabUrlLoader, RenkuUrlLoader}
import io.renku.graph.model._
import io.renku.http.rest.paging.{PagingHeaders, PagingResponse}
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.{Header, Request, Response, Status}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait Endpoint[F[_]] {
  def `GET /entities`(criteria: Criteria, request: Request[F]): F[Response[F]]
}

object Endpoint {

  def apply[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder]: F[Endpoint[F]] = for {
    entitiesFinder <- EntitiesFinder[F]
    renkuUrl       <- RenkuUrlLoader()
    renkuApiUrl    <- renku.ApiUrl()
    gitLabUrl      <- GitLabUrlLoader[F]()
  } yield new EndpointImpl(entitiesFinder, renkuUrl, renkuApiUrl, gitLabUrl)
}

private class EndpointImpl[F[_]: Async: Logger](
    finder:                   EntitiesFinder[F],
    implicit val renkuUrl:    RenkuUrl,
    implicit val renkuApiUrl: renku.ApiUrl,
    implicit val gitLabUrl:   GitLabUrl
) extends Http4sDsl[F]
    with Endpoint[F]
    with ModelEncoders {

  override def `GET /entities`(criteria: Criteria, request: Request[F]): F[Response[F]] =
    finder
      .findEntities(criteria)
      .map(toHttpResponse(request))
      .recoverWith(httpResult)

  private def toHttpResponse(request: Request[F])(response: PagingResponse[model.Entity]): Response[F] = {
    val resourceUrl: renku.ResourceUrl = renku.ResourceUrl(show"$renkuUrl${request.uri}")
    Response[F](Status.Ok)
      .withEntity(response.results.asJson)
      .putHeaders(PagingHeaders.from(response)(resourceUrl, ResourceUrl).toSeq.map(Header.ToRaw.rawToRaw): _*)
  }

  private lazy val httpResult: PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = Message.Error("Cross-entity search failed")
    Logger[F].error(exception)(errorMessage.show) >> InternalServerError(errorMessage)
  }
}
