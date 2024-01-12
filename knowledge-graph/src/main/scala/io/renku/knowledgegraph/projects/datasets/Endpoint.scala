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

package io.renku.knowledgegraph.projects.datasets

import Endpoint.Criteria
import cats.NonEmptyParallel
import cats.effect._
import cats.syntax.all._
import io.circe.Encoder
import io.circe.syntax._
import io.renku.config.renku
import io.renku.data.Message
import io.renku.graph.config.{GitLabUrlLoader, RenkuUrlLoader}
import io.renku.graph.model.{GitLabUrl, RenkuUrl, projects}
import io.renku.http.RenkuEntityCodec
import io.renku.http.rest.Links._
import io.renku.http.rest.SortBy.Direction
import io.renku.http.rest.Sorting
import io.renku.http.rest.paging.{PagingHeaders, PagingRequest, PagingResponse}
import io.renku.logging.{ExecutionTimeRecorder, ExecutionTimeRecorderLoader}
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.http4s.dsl.Http4sDsl
import org.http4s.{Header, Request, Response}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait Endpoint[F[_]] {
  def `GET /projects/:slug/datasets`(request: Request[F], criteria: Criteria): F[Response[F]]
}

class EndpointImpl[F[_]: MonadCancelThrow: Logger](
    projectDatasetsFinder: ProjectDatasetsFinder[F],
    executionTimeRecorder: ExecutionTimeRecorder[F]
)(implicit renkuUrl: RenkuUrl, renkuApiUrl: renku.ApiUrl, gitLabUrl: GitLabUrl)
    extends Http4sDsl[F]
    with RenkuEntityCodec
    with Endpoint[F] {

  import executionTimeRecorder._

  def `GET /projects/:slug/datasets`(request: Request[F], criteria: Criteria): F[Response[F]] =
    measureAndLogTime(finishedSuccessfully(criteria.projectSlug)) {
      projectDatasetsFinder
        .findProjectDatasets(criteria)
        .map(toHttpResponse(request, criteria))
        .recoverWith(errorHttpResponse(criteria.projectSlug))
    }

  private def toHttpResponse(request: Request[F], criteria: Criteria)(
      response: PagingResponse[ProjectDataset]
  ): Response[F] = {
    implicit val encoder: Encoder[ProjectDataset] = ProjectDataset.encoder(criteria.projectSlug)
    val resourceUrl = renku.ResourceUrl(show"$renkuUrl${request.uri}")
    Response[F](Ok)
      .withEntity(response.results.asJson)
      .putHeaders(PagingHeaders.from(response)(resourceUrl, renku.ResourceUrl).toSeq.map(Header.ToRaw.rawToRaw): _*)
  }

  private def errorHttpResponse(
      projectSlug: projects.Slug
  ): PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = Message.Error.unsafeApply(s"Finding $projectSlug's datasets failed")
    Logger[F].error(exception)(errorMessage.show) >> InternalServerError(errorMessage)
  }

  private def finishedSuccessfully(projectSlug: projects.Slug): PartialFunction[Response[F], String] = {
    case response if response.status == Ok => s"Finding '$projectSlug' datasets finished"
  }
}

object Endpoint {

  final case class Criteria(projectSlug: projects.Slug,
                            sorting:     Sorting[Criteria.Sort.type] = Criteria.Sort.default,
                            paging:      PagingRequest = PagingRequest.default
  )

  object Criteria {
    object Sort extends io.renku.http.rest.SortBy {

      type PropertyType = SortProperty

      sealed trait SortProperty extends Property

      final case object ByName         extends Property("name") with SortProperty
      final case object ByDateModified extends Property("dateModified") with SortProperty

      val byNameAsc: Sort.By = Sort.By(ByName, Direction.Asc)

      val default: Sorting[Sort.type] = Sorting(byNameAsc)

      override lazy val properties: Set[SortProperty] = Set(ByName, ByDateModified)
    }
  }

  def apply[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder]: F[Endpoint[F]] = for {
    implicit0(renkuUrl: RenkuUrl)        <- RenkuUrlLoader()
    implicit0(gitLabUrl: GitLabUrl)      <- GitLabUrlLoader[F]()
    implicit0(renkuApiUrl: renku.ApiUrl) <- renku.ApiUrl[F]()
    renkuConnectionConfig                <- ProjectsConnectionConfig.fromConfig[F]()
    executionTimeRecorder                <- ExecutionTimeRecorderLoader[F]()
  } yield new EndpointImpl[F](ProjectDatasetsFinder(renkuConnectionConfig), executionTimeRecorder)

  def href(renkuApiUrl: renku.ApiUrl, projectSlug: projects.Slug): Href =
    Href(renkuApiUrl / "projects" / projectSlug / "datasets")
}
