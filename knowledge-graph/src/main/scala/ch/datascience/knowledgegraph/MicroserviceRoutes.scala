/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph

import cats.data.{Validated, ValidatedNel}
import cats.effect.{Clock, ConcurrentEffect}
import cats.implicits._
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.http.rest.SortBy.Direction
import ch.datascience.http.rest.paging.PagingRequest
import ch.datascience.http.rest.paging.PagingRequest.Decoders._
import ch.datascience.http.rest.paging.model.{Page, PerPage}
import ch.datascience.http.server.QueryParameterTools._
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import ch.datascience.knowledgegraph.datasets.rest._
import ch.datascience.knowledgegraph.graphql.QueryEndpoint
import ch.datascience.knowledgegraph.projects.rest.ProjectEndpoint
import ch.datascience.metrics.RoutesMetrics
import org.http4s.dsl.Http4sDsl
import org.http4s.{ParseFailure, Response}

import scala.language.higherKinds

private class MicroserviceRoutes[F[_]: ConcurrentEffect](
    queryEndpoint:           QueryEndpoint[F],
    projectEndpoint:         ProjectEndpoint[F],
    projectDatasetsEndpoint: ProjectDatasetsEndpoint[F],
    datasetEndpoint:         DatasetEndpoint[F],
    datasetsSearchEndpoint:  DatasetsSearchEndpoint[F],
    routesMetrics:           RoutesMetrics[F]
)(implicit clock:            Clock[F])
    extends Http4sDsl[F] {

  import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.query
  import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
  import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort._
  import datasetEndpoint._
  import org.http4s.HttpRoutes
  import projectDatasetsEndpoint._
  import projectEndpoint._
  import queryEndpoint._
  import routesMetrics._

  // format: off
  lazy val routes: F[HttpRoutes[F]] = HttpRoutes.of[F] {
    case           GET  -> Root / "ping"                                                                                                      => Ok("pong")
    case           GET  -> Root / "knowledge-graph" / "datasets" :? query(maybePhrase) +& sort(maybeSortBy) +& page(page) +& perPage(perPage) => searchForDatasets(maybePhrase, maybeSortBy,page, perPage)
    case           GET  -> Root / "knowledge-graph" / "datasets" / DatasetId(id)                                                              => getDataset(id)
    case           GET  -> Root / "knowledge-graph" / "graphql"                                                                               => schema
    case request @ POST -> Root / "knowledge-graph" / "graphql"                                                                               => handleQuery(request)
    case           GET  ->        "knowledge-graph" /: "projects" /: path                                                                     => routeToProjectsEndpoints(path)
  }.meter flatMap `add GET Root / metrics`
  // format: on

  private def searchForDatasets(
      maybePhrase:  Option[ValidatedNel[ParseFailure, DatasetsSearchEndpoint.Query.Phrase]],
      maybeSort:    Option[ValidatedNel[ParseFailure, DatasetsSearchEndpoint.Sort.By]],
      maybePage:    Option[ValidatedNel[ParseFailure, Page]],
      maybePerPage: Option[ValidatedNel[ParseFailure, PerPage]]
  ): F[Response[F]] =
    (maybePhrase.map(_.map(Option.apply)).getOrElse(Validated.validNel(Option.empty[Phrase])),
     maybeSort getOrElse Validated.validNel(Sort.By(NameProperty, Direction.Asc)),
     PagingRequest(maybePage, maybePerPage))
      .mapN(datasetsSearchEndpoint.searchForDatasets)
      .fold(toBadRequest(), identity)

  private def routeToProjectsEndpoints(path: Path): F[Response[F]] = path.toList match {
    case projectPathParts :+ "datasets" => projectPathParts.toProjectPath.fold(identity, getProjectDatasets)
    case projectPathParts               => projectPathParts.toProjectPath.fold(identity, getProject)
  }

  private implicit class PathPartsOps(parts: List[String]) {
    import cats.MonadError
    import cats.implicits._
    import ch.datascience.controllers.InfoMessage
    import ch.datascience.controllers.InfoMessage._
    import org.http4s.{Response, Status}

    private implicit val ME: MonadError[F, Throwable] = implicitly[MonadError[F, Throwable]]

    lazy val toProjectPath: Either[F[Response[F]], ProjectPath] =
      ProjectPath
        .from(parts.mkString("/"))
        .leftMap(_ => Response[F](Status.NotFound).withEntity(InfoMessage("Resource not found")).pure[F])
  }
}
