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

package io.renku.knowledgegraph
package projects.datasets.tags

import Endpoint._
import cats.NonEmptyParallel
import cats.effect.Async
import cats.syntax.all._
import io.circe.Encoder
import io.renku.config.renku
import io.renku.graph
import io.renku.graph.model.RenkuUrl
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
                            datasetName: graph.model.datasets.Name,
                            paging:      PagingRequest = PagingRequest.default,
                            maybeUser:   Option[AuthUser] = None
  )

  import io.renku.graph.config.RenkuUrlLoader

  def apply[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder]: F[Endpoint[F]] = for {
    tagsFinder  <- TagsFinder[F]
    renkuUrl    <- RenkuUrlLoader()
    renkuApiUrl <- renku.ApiUrl()
  } yield new EndpointImpl(tagsFinder, renkuUrl, renkuApiUrl)
}

private class EndpointImpl[F[_]: Async: Logger](tagsFinder: TagsFinder[F],
                                                renkuUrl:    RenkuUrl,
                                                renkuApiUrl: renku.ApiUrl
) extends Http4sDsl[F]
    with Endpoint[F] {

  import io.circe.Json
  import io.circe.literal._
  import io.circe.syntax._
  import io.renku.http.ErrorMessage
  import io.renku.http.ErrorMessage._
  import io.renku.http.rest.Links._
  import io.renku.http.rest.paging.{PagingHeaders, PagingResponse}
  import io.renku.json.JsonOps._
  import io.renku.tinytypes.json.TinyTypeEncoders._
  import org.http4s.circe.jsonEncoderOf
  import org.http4s.{EntityEncoder, Header, Status}

  import scala.util.control.NonFatal

  override def `GET /projects/:path/datasets/:name/tags`(criteria: Criteria)(implicit
      request:                                                     Request[F]
  ): F[Response[F]] = tagsFinder.findTags(criteria) map toHttpResponse(request) recoverWith httpResult

  private def toHttpResponse(request: Request[F])(response: PagingResponse[model.Tag]): Response[F] = {
    implicit val resourceUrl: renku.ResourceUrl = renku.ResourceUrl(show"$renkuUrl${request.uri}")
    Response[F](Status.Ok)
      .withEntity(response.results.asJson)
      .putHeaders(PagingHeaders.from(response).toSeq.map(Header.ToRaw.rawToRaw): _*)
  }

  private implicit lazy val modelEncoder: Encoder[model.Tag] = Encoder.instance { tag =>
    json"""{
      "name": ${tag.name},
      "date": ${tag.startDate}
    }"""
      .addIfDefined("description" -> tag.maybeDesc)
      .deepMerge(
        _links(
          Link(Rel("dataset-details") -> datasets.details.DatasetEndpoint.href(renkuApiUrl, tag.datasetId))
        )
      )
  }

  private implicit lazy val responseEntityEncoder: EntityEncoder[F, Json] = jsonEncoderOf[F, Json]

  private lazy val httpResult: PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage("Project Dataset Tags search failed")
    Logger[F].error(exception)(errorMessage.value) >> InternalServerError(errorMessage)
  }
}
