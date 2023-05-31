/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph
package projects.datasets.tags

import Endpoint._
import cats.NonEmptyParallel
import cats.effect.Async
import cats.syntax.all._
import io.renku.config.renku
import io.renku.graph
import io.renku.graph.model.{RenkuUrl, datasets}
import io.renku.http.rest.Links.Href
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response}
import org.typelevel.log4cats.Logger

trait Endpoint[F[_]] {
  def `GET /projects/:path/datasets/:name/tags`(criteria: Criteria)(implicit request: Request[F]): F[Response[F]]
}

object Endpoint {

  final case class Criteria(projectPath: graph.model.projects.Path,
                            datasetSlug: graph.model.datasets.Slug,
                            paging:      PagingRequest = PagingRequest.default,
                            maybeUser:   Option[AuthUser] = None
  )

  import io.renku.graph.config.RenkuUrlLoader

  def apply[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder]: F[Endpoint[F]] = for {
    implicit0(renkuUrl: RenkuUrl) <- RenkuUrlLoader()
    tagsFinder                    <- TagsFinder[F]
    renkuApiUrl                   <- renku.ApiUrl()
  } yield new EndpointImpl(tagsFinder, renkuUrl, renkuApiUrl)

  def href(renkuApiUrl: renku.ApiUrl, projectPath: graph.model.projects.Path, slug: datasets.Slug): Href =
    Href(renkuApiUrl / "projects" / projectPath / "datasets" / slug / "tags")
}

private class EndpointImpl[F[_]: Async: Logger](tagsFinder: TagsFinder[F],
                                                renkuUrl:    RenkuUrl,
                                                renkuApiUrl: renku.ApiUrl
) extends Http4sDsl[F]
    with Endpoint[F] {

  import io.circe.Encoder._
  import io.circe.Json
  import io.circe.syntax._
  import io.renku.http.ErrorMessage
  import io.renku.http.ErrorMessage._
  import io.renku.http.rest.paging.{PagingHeaders, PagingResponse}
  import org.http4s.circe.jsonEncoderOf
  import org.http4s.{EntityEncoder, Header, Status}

  import scala.util.control.NonFatal

  private implicit val apiUrl: renku.ApiUrl = renkuApiUrl

  override def `GET /projects/:path/datasets/:name/tags`(criteria: Criteria)(implicit
      request: Request[F]
  ): F[Response[F]] = tagsFinder.findTags(criteria) map toHttpResponse(request) recoverWith httpResult

  private def toHttpResponse(request: Request[F])(response: PagingResponse[model.Tag]): Response[F] = {
    val resourceUrl = renku.ResourceUrl(show"$renkuUrl${request.uri}")
    Response[F](Status.Ok)
      .withEntity(response.results.asJson)
      .putHeaders(PagingHeaders.from(response)(resourceUrl, renku.ResourceUrl).toSeq.map(Header.ToRaw.rawToRaw): _*)
  }

  private implicit lazy val responseEntityEncoder: EntityEncoder[F, Json] = jsonEncoderOf[F, Json]

  private lazy val httpResult: PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage("Project Dataset Tags search failed")
    Logger[F].error(exception)(errorMessage.value) >> InternalServerError(errorMessage)
  }
}
