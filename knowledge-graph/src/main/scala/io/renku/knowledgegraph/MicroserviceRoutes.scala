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

import cats.MonadThrow
import cats.data.{EitherT, Validated, ValidatedNel}
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import cats.syntax.all._
import io.renku.config.GitLab
import io.renku.control.{RateLimit, Throttler}
import io.renku.graph.http.server.security._
import io.renku.graph.model
import io.renku.http.client.GitLabClient
import io.renku.http.rest.SortBy.Direction
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.PagingRequest.Decoders._
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.server.QueryParameterTools._
import io.renku.http.server.security.Authentication
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import io.renku.knowledgegraph.datasets.rest._
import io.renku.knowledgegraph.graphql.QueryEndpoint
import io.renku.knowledgegraph.projects.rest.ProjectEndpoint
import io.renku.metrics.{MetricsRegistry, RoutesMetrics}
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.http4s.dsl.Http4sDsl
import org.http4s.server.AuthMiddleware
import org.http4s.{AuthedRoutes, ParseFailure, Response}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private class MicroserviceRoutes[F[_]: MonadThrow](
    queryEndpoint:           QueryEndpoint[F],
    projectEndpoint:         ProjectEndpoint[F],
    projectDatasetsEndpoint: ProjectDatasetsEndpoint[F],
    datasetEndpoint:         DatasetEndpoint[F],
    datasetsSearchEndpoint:  DatasetsSearchEndpoint[F],
    authMiddleware:          AuthMiddleware[F, Option[AuthUser]],
    projectPathAuthorizer:   Authorizer[F, model.projects.Path],
    datasetIdAuthorizer:     Authorizer[F, model.datasets.Identifier],
    routesMetrics:           RoutesMetrics[F]
) extends Http4sDsl[F] {

  import datasetEndpoint._
  import datasetIdAuthorizer.{authorize => authorizeDatasetId}
  import io.renku.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.query
  import io.renku.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
  import io.renku.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort._
  import org.http4s.HttpRoutes
  import projectDatasetsEndpoint._
  import projectEndpoint._
  import projectPathAuthorizer.{authorize => authorizePath}
  import queryEndpoint._
  import routesMetrics._

  // format: off
  private lazy val authorizedRoutes: HttpRoutes[F] = authMiddleware {
    AuthedRoutes.of {
      case GET            -> Root / "knowledge-graph" /  "datasets" :? query(maybePhrase) +& sort(maybeSortBy) +& page(page) +& perPage(perPage) as maybeUser => searchForDatasets(maybePhrase, maybeSortBy,page, perPage, maybeUser)
      case GET            -> Root / "knowledge-graph" /  "datasets" / DatasetId(id)                                                              as maybeUser => fetchDataset(id, maybeUser)
      case authReq @ POST -> Root / "knowledge-graph" /  "graphql"                                                                               as maybeUser => handleQuery(authReq.req, maybeUser)
      case GET ->                   "knowledge-graph" /: "projects" /: path                                                                      as maybeUser => routeToProjectsEndpoints(path, maybeUser)
    }
  }

  private lazy val nonAuthorizedRoutes: HttpRoutes[F] = HttpRoutes.of[F] {
    case         GET  -> Root / "ping"                                                                                                       => Ok("pong")
    case         GET ->  Root / "knowledge-graph" /  "graphql"                                                                               => schema()
  }
  // format: on

  lazy val routes: Resource[F, HttpRoutes[F]] = (nonAuthorizedRoutes <+> authorizedRoutes).withMetrics

  private def searchForDatasets(
      maybePhrase:   Option[ValidatedNel[ParseFailure, DatasetsSearchEndpoint.Query.Phrase]],
      maybeSort:     Option[ValidatedNel[ParseFailure, DatasetsSearchEndpoint.Sort.By]],
      maybePage:     Option[ValidatedNel[ParseFailure, Page]],
      maybePerPage:  Option[ValidatedNel[ParseFailure, PerPage]],
      maybeAuthUser: Option[AuthUser]
  ): F[Response[F]] =
    (maybePhrase.map(_.map(Option.apply)).getOrElse(Validated.validNel(Option.empty[Phrase])),
     maybeSort getOrElse Validated.validNel(Sort.By(TitleProperty, Direction.Asc)),
     PagingRequest(maybePage, maybePerPage)
    ).mapN { case (maybePhrase, sort, paging) =>
      datasetsSearchEndpoint.searchForDatasets(maybePhrase, sort, paging, maybeAuthUser)
    }.fold(toBadRequest, identity)

  private def fetchDataset(datasetId: model.datasets.Identifier, maybeAuthUser: Option[AuthUser]): F[Response[F]] =
    authorizeDatasetId(datasetId, maybeAuthUser)
      .leftMap(_.toHttpResponse[F])
      .semiflatMap(getDataset(datasetId, _))
      .merge

  private def routeToProjectsEndpoints(
      path:          Path,
      maybeAuthUser: Option[AuthUser]
  ): F[Response[F]] = path.segments.toList.map(_.toString) match {
    case projectPathParts :+ "datasets" =>
      projectPathParts.toProjectPath
        .flatTap(authorizePath(_, maybeAuthUser).leftMap(_.toHttpResponse))
        .semiflatMap(getProjectDatasets)
        .merge
    case projectPathParts =>
      projectPathParts.toProjectPath
        .flatTap(authorizePath(_, maybeAuthUser).leftMap(_.toHttpResponse))
        .semiflatMap(getProject(_, maybeAuthUser))
        .merge
  }

  private implicit class PathPartsOps(parts: List[String]) {
    import io.renku.http.InfoMessage
    import io.renku.http.InfoMessage._
    import org.http4s.{Response, Status}

    lazy val toProjectPath: EitherT[F, Response[F], model.projects.Path] = EitherT.fromEither[F] {
      model.projects.Path
        .from(parts mkString "/")
        .leftMap(_ => Response[F](Status.NotFound).withEntity(InfoMessage("Resource not found")))
    }
  }
}

private object MicroserviceRoutes {

  def apply(sparqlTimeRecorder: SparqlQueryTimeRecorder[IO])(implicit
      executionContext:         ExecutionContext,
      runtime:                  IORuntime,
      logger:                   Logger[IO],
      metricsRegistry:          MetricsRegistry[IO]
  ): IO[MicroserviceRoutes[IO]] =
    for {
      gitLabRateLimit         <- RateLimit.fromConfig[IO, GitLab]("services.gitlab.rate-limit")
      gitLabThrottler         <- Throttler[IO, GitLab](gitLabRateLimit)
      gitLabClient            <- GitLabClient(gitLabThrottler)
      queryEndpoint           <- QueryEndpoint(sparqlTimeRecorder)
      projectEndpoint         <- ProjectEndpoint[IO](gitLabClient, sparqlTimeRecorder)
      projectDatasetsEndpoint <- ProjectDatasetsEndpoint[IO](sparqlTimeRecorder)
      datasetEndpoint         <- DatasetEndpoint[IO](sparqlTimeRecorder)
      datasetsSearchEndpoint  <- DatasetsSearchEndpoint[IO](sparqlTimeRecorder)
      authenticator           <- GitLabAuthenticator(gitLabThrottler)
      authMiddleware          <- Authentication.middlewareAuthenticatingIfNeeded(authenticator)
      projectPathAuthorizer   <- Authorizer.using(ProjectPathRecordsFinder[IO](sparqlTimeRecorder))
      datasetIdAuthorizer     <- Authorizer.using(DatasetIdRecordsFinder[IO](sparqlTimeRecorder))
    } yield new MicroserviceRoutes(queryEndpoint,
                                   projectEndpoint,
                                   projectDatasetsEndpoint,
                                   datasetEndpoint,
                                   datasetsSearchEndpoint,
                                   authMiddleware,
                                   projectPathAuthorizer,
                                   datasetIdAuthorizer,
                                   new RoutesMetrics[IO]
    )
}
