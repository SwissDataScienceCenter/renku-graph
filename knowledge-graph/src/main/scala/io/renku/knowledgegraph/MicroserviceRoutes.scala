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
import io.renku.graph.model.persons
import io.renku.http.client.GitLabClient
import io.renku.http.rest.SortBy.Direction
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.PagingRequest.Decoders._
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.server.QueryParameterTools._
import io.renku.http.server.security.Authentication
import io.renku.http.server.security.model.AuthUser
import io.renku.http.server.version
import io.renku.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import io.renku.knowledgegraph.datasets.rest._
import io.renku.knowledgegraph.graphql.QueryEndpoint
import io.renku.knowledgegraph.projects.rest.ProjectEndpoint
import io.renku.metrics.{MetricsRegistry, RoutesMetrics}
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.http4s.dsl.Http4sDsl
import org.http4s.server.AuthMiddleware
import org.http4s.{AuthedRoutes, ParseFailure, Request, Response}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private class MicroserviceRoutes[F[_]: MonadThrow](
    datasetsSearchEndpoint:  DatasetsSearchEndpoint[F],
    datasetEndpoint:         DatasetEndpoint[F],
    entitiesEndpoint:        entities.Endpoint[F],
    queryEndpoint:           QueryEndpoint[F],
    projectEndpoint:         ProjectEndpoint[F],
    projectDatasetsEndpoint: ProjectDatasetsEndpoint[F],
    authMiddleware:          AuthMiddleware[F, Option[AuthUser]],
    projectPathAuthorizer:   Authorizer[F, model.projects.Path],
    datasetIdAuthorizer:     Authorizer[F, model.datasets.Identifier],
    routesMetrics:           RoutesMetrics[F],
    versionRoutes:           version.Routes[F]
) extends Http4sDsl[F] {

  import datasetEndpoint._
  import datasetIdAuthorizer.{authorize => authorizeDatasetId}
  import entitiesEndpoint._
  import org.http4s.HttpRoutes
  import projectDatasetsEndpoint._
  import projectEndpoint._
  import projectPathAuthorizer.{authorize => authorizePath}
  import queryEndpoint._
  import routesMetrics._

  lazy val routes: Resource[F, HttpRoutes[F]] =
    (versionRoutes() <+> nonAuthorizedRoutes <+> authorizedRoutes).withMetrics

  private lazy val authorizedRoutes: HttpRoutes[F] = authMiddleware {
    `GET /dataset routes` <+> `GET /entities routes` <+> otherAuthRoutes
  }

  private lazy val `GET /dataset routes`: AuthedRoutes[Option[AuthUser], F] = {
    import io.renku.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.query
    import io.renku.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort.sort

    AuthedRoutes.of {
      case GET -> Root / "knowledge-graph" / "datasets"
          :? query(maybePhrase) +& sort(maybeSortBy) +& page(page) +& perPage(perPage) as maybeUser =>
        searchForDatasets(maybePhrase, maybeSortBy, page, perPage, maybeUser)
      case GET -> Root / "knowledge-graph" / "datasets" / DatasetId(id) as maybeUser => fetchDataset(id, maybeUser)
    }
  }

  // format: off
  private lazy val `GET /entities routes`: AuthedRoutes[Option[AuthUser], F] = {
    import io.renku.knowledgegraph.entities.Endpoint.Criteria._
    import Filters.CreatorName.creatorNames
    import Filters.Date.date
    import Filters.EntityType.entityTypes
    import Filters.Query.query
    import Filters.Visibility.visibilities
    import Sorting.sort

    AuthedRoutes.of {
      case req @ GET -> Root / "knowledge-graph" / "entities"
        :? query(maybeQuery) +& entityTypes(maybeTypes) +& creatorNames(maybeCreators)
        +& visibilities(maybeVisibilities) +& date(maybeDate) +& sort(maybeSort)
        +& page(maybePage) +& perPage(maybePerPage) as maybeUser =>
        searchForEntities(maybeQuery, maybeTypes, maybeCreators, maybeVisibilities,
          maybeDate, maybeSort, maybePage, maybePerPage, maybeUser, req.req)
    }
  }
  // format: on

  private lazy val otherAuthRoutes: AuthedRoutes[Option[AuthUser], F] = AuthedRoutes.of {
    case authReq @ POST -> Root / "knowledge-graph" / "graphql" as maybeUser => handleQuery(authReq.req, maybeUser)
    case GET -> "knowledge-graph" /: "projects" /: path as maybeUser => routeToProjectsEndpoints(path, maybeUser)
  }

  private lazy val nonAuthorizedRoutes: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "knowledge-graph" / "graphql" => schema()
    case GET -> Root / "ping"                        => Ok("pong")
  }

  private def searchForDatasets(
      maybePhrase:   Option[ValidatedNel[ParseFailure, DatasetsSearchEndpoint.Query.Phrase]],
      maybeSort:     Option[ValidatedNel[ParseFailure, DatasetsSearchEndpoint.Sort.By]],
      maybePage:     Option[ValidatedNel[ParseFailure, Page]],
      maybePerPage:  Option[ValidatedNel[ParseFailure, PerPage]],
      maybeAuthUser: Option[AuthUser]
  ): F[Response[F]] = {
    import io.renku.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
    import io.renku.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort._

    (maybePhrase.map(_.map(Option.apply)).getOrElse(Validated.validNel(Option.empty[Phrase])),
     maybeSort getOrElse Validated.validNel(Sort.By(TitleProperty, Direction.Asc)),
     PagingRequest(maybePage, maybePerPage)
    ).mapN { case (maybePhrase, sort, paging) =>
      datasetsSearchEndpoint.searchForDatasets(maybePhrase, sort, paging, maybeAuthUser)
    }.fold(toBadRequest, identity)
  }

  private def searchForEntities(
      maybeQuery:   Option[ValidatedNel[ParseFailure, entities.Endpoint.Criteria.Filters.Query]],
      types:        ValidatedNel[ParseFailure, List[entities.Endpoint.Criteria.Filters.EntityType]],
      creators:     ValidatedNel[ParseFailure, List[persons.Name]],
      visibilities: ValidatedNel[ParseFailure, List[model.projects.Visibility]],
      maybeDate:    Option[ValidatedNel[ParseFailure, entities.Endpoint.Criteria.Filters.Date]],
      maybeSort:    Option[ValidatedNel[ParseFailure, entities.Endpoint.Criteria.Sorting.By]],
      maybePage:    Option[ValidatedNel[ParseFailure, Page]],
      maybePerPage: Option[ValidatedNel[ParseFailure, PerPage]],
      maybeUser:    Option[AuthUser],
      request:      Request[F]
  ): F[Response[F]] = {
    import entities.Endpoint.Criteria
    import entities.Endpoint.Criteria.Filters._
    import entities.Endpoint.Criteria.Sorting._
    import entities.Endpoint.Criteria.{Filters, Sorting}
    (
      maybeQuery.map(_.map(Option.apply)).getOrElse(Validated.validNel(Option.empty[Query])),
      types.map(_.toSet),
      creators.map(_.toSet),
      visibilities.map(_.toSet),
      maybeDate.map(_.map(Option.apply)).getOrElse(Validated.validNel(Option.empty[Date])),
      maybeSort getOrElse Validated.validNel(Sorting.By(ByName, Direction.Asc)),
      PagingRequest(maybePage, maybePerPage)
    ).mapN { case (maybeQuery, types, creators, visibilities, maybeDate, sorting, paging) =>
      `GET /entities`(
        Criteria(Filters(maybeQuery, types, creators, visibilities, maybeDate), sorting, paging, maybeUser),
        request
      )
    }.fold(toBadRequest, identity)
  }

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

  def apply()(implicit
      executionContext:   ExecutionContext,
      runtime:            IORuntime,
      logger:             Logger[IO],
      metricsRegistry:    MetricsRegistry[IO],
      sparqlTimeRecorder: SparqlQueryTimeRecorder[IO]
  ): IO[MicroserviceRoutes[IO]] = for {
    gitLabRateLimit         <- RateLimit.fromConfig[IO, GitLab]("services.gitlab.rate-limit")
    gitLabThrottler         <- Throttler[IO, GitLab](gitLabRateLimit)
    datasetsSearchEndpoint  <- DatasetsSearchEndpoint[IO]
    datasetEndpoint         <- DatasetEndpoint[IO]
    entitiesEndpoint        <- entities.Endpoint[IO]
    gitLabClient            <- GitLabClient(gitLabThrottler)
    queryEndpoint           <- QueryEndpoint()
    projectEndpoint         <- ProjectEndpoint[IO](gitLabClient)
    projectDatasetsEndpoint <- ProjectDatasetsEndpoint[IO]
    authenticator           <- GitLabAuthenticator(gitLabClient)
    authMiddleware          <- Authentication.middlewareAuthenticatingIfNeeded(authenticator)
    projectPathAuthorizer   <- Authorizer.using(ProjectPathRecordsFinder[IO])
    datasetIdAuthorizer     <- Authorizer.using(DatasetIdRecordsFinder[IO])
    versionRoutes           <- version.Routes[IO]
  } yield new MicroserviceRoutes(datasetsSearchEndpoint,
                                 datasetEndpoint,
                                 entitiesEndpoint,
                                 queryEndpoint,
                                 projectEndpoint,
                                 projectDatasetsEndpoint,
                                 authMiddleware,
                                 projectPathAuthorizer,
                                 datasetIdAuthorizer,
                                 new RoutesMetrics[IO],
                                 versionRoutes
  )
}
