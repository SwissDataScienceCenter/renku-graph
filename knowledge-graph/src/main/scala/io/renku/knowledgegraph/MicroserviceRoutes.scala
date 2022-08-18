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

import cats.data.Validated.Valid
import cats.data.{EitherT, Validated, ValidatedNel}
import cats.effect.unsafe.IORuntime
import cats.effect.{Async, IO, Resource}
import cats.syntax.all._
import io.renku.graph.http.server.security._
import io.renku.graph.model
import io.renku.graph.model.persons
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.InfoMessage
import io.renku.http.InfoMessage._
import io.renku.http.client.GitLabClient
import io.renku.http.rest.SortBy.Direction
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.PagingRequest.Decoders._
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.server.QueryParameterTools._
import io.renku.http.server.security.Authentication
import io.renku.http.server.security.model.AuthUser
import io.renku.http.server.version
import io.renku.knowledgegraph.datasets.DatasetsSearchEndpoint.Query.Phrase
import io.renku.knowledgegraph.datasets._
import io.renku.knowledgegraph.graphql.QueryEndpoint
import io.renku.metrics.{MetricsRegistry, RoutesMetrics}
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.http4s.dsl.Http4sDsl
import org.http4s.server.AuthMiddleware
import org.http4s.{AuthedRoutes, ParseFailure, Request, Response, Status, Uri}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private class MicroserviceRoutes[F[_]: Async](
    datasetsSearchEndpoint:  DatasetsSearchEndpoint[F],
    datasetEndpoint:         DatasetEndpoint[F],
    entitiesEndpoint:        entities.Endpoint[F],
    queryEndpoint:           QueryEndpoint[F],
    lineageEndpoint:         projects.files.lineage.Endpoint[F],
    ontologyEndpoint:        ontology.Endpoint[F],
    projectDetailsEndpoint:  projects.details.Endpoint[F],
    projectDatasetsEndpoint: ProjectDatasetsEndpoint[F],
    docsEndpoint:            docs.Endpoint[F],
    usersProjectsEndpoint:   users.projects.Endpoint[F],
    authMiddleware:          AuthMiddleware[F, Option[AuthUser]],
    projectPathAuthorizer:   Authorizer[F, model.projects.Path],
    datasetIdAuthorizer:     Authorizer[F, model.datasets.Identifier],
    routesMetrics:           RoutesMetrics[F],
    versionRoutes:           version.Routes[F]
) extends Http4sDsl[F] {

  import datasetEndpoint._
  import datasetIdAuthorizer.{authorize => authorizeDatasetId}
  import entitiesEndpoint._
  import lineageEndpoint._
  import ontologyEndpoint._
  import org.http4s.HttpRoutes
  import projectDatasetsEndpoint._
  import projectDetailsEndpoint._
  import projectPathAuthorizer.{authorize => authorizePath}
  import queryEndpoint._
  import routesMetrics._

  lazy val routes: Resource[F, HttpRoutes[F]] =
    (versionRoutes() <+> nonAuthorizedRoutes <+> authorizedRoutes).withMetrics

  private lazy val authorizedRoutes: HttpRoutes[F] = authMiddleware {
    `GET /dataset routes` <+> `GET /entities routes` <+> `GET /users/:id/project route` <+> otherAuthRoutes
  }

  private lazy val `GET /dataset routes`: AuthedRoutes[Option[AuthUser], F] = {
    import io.renku.knowledgegraph.datasets.DatasetsSearchEndpoint.Query.query
    import io.renku.knowledgegraph.datasets.DatasetsSearchEndpoint.Sort.sort

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
    import Filters.EntityType.entityTypes
    import Filters.Query.query
    import Filters.Since.since
    import Filters.Until.until
    import Filters.Visibility.visibilities
    import Sorting.sort

    AuthedRoutes.of {
      case req@GET -> Root / "knowledge-graph" / "entities"
        :? query(maybeQuery) +& entityTypes(maybeTypes) +& creatorNames(maybeCreators)
        +& visibilities(maybeVisibilities) +& since(maybeSince) +& until(maybeUntil) 
        +& sort(maybeSort) +& page(maybePage) +& perPage(maybePerPage) as maybeUser =>
        searchForEntities(maybeQuery, maybeTypes, maybeCreators, maybeVisibilities,
          maybeSince, maybeUntil, maybeSort, maybePage, maybePerPage, maybeUser, req.req)
    }
  }
  // format: on

  private lazy val `GET /users/:id/project route`: AuthedRoutes[Option[AuthUser], F] = {
    import users.binders._
    import users.projects.Endpoint._
    import Criteria.Filters
    import Criteria.Filters.ActivationState._
    import Criteria.Filters._
    import usersProjectsEndpoint._

    AuthedRoutes.of {
      case req @ GET -> Root / "knowledge-graph" / "users" / GitLabId(id) / "projects"
          :? activationState(maybeState) +& page(maybePage) +& perPage(maybePerPage) as maybeUser =>
        (maybeState getOrElse ActivationState.All.validNel, PagingRequest(maybePage, maybePerPage))
          .mapN { (activationState, paging) =>
            `GET /users/:id/projects`(Criteria(userId = id, Filters(activationState), paging, maybeUser), req.req)
          }
          .fold(toBadRequest, identity)
    }
  }

  private lazy val otherAuthRoutes: AuthedRoutes[Option[AuthUser], F] = AuthedRoutes.of {
    case authReq @ POST -> Root / "knowledge-graph" / "graphql" as maybeUser => handleQuery(authReq.req, maybeUser)
    case authReq @ GET -> "knowledge-graph" /: "projects" /: path as maybeUser =>
      routeToProjectsEndpoints(path, maybeUser)(authReq.req)
  }

  private lazy val nonAuthorizedRoutes: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "knowledge-graph" / "graphql"          => schema()
    case req @ GET -> "knowledge-graph" /: "ontology" /: path => `GET /ontology`(path)(req)
    case GET -> Root / "knowledge-graph" / "spec.json"        => docsEndpoint.`get /spec.json`
    case GET -> Root / "ping"                                 => Ok("pong")
  }

  private def searchForDatasets(
      maybePhrase:   Option[ValidatedNel[ParseFailure, DatasetsSearchEndpoint.Query.Phrase]],
      maybeSort:     Option[ValidatedNel[ParseFailure, DatasetsSearchEndpoint.Sort.By]],
      maybePage:     Option[ValidatedNel[ParseFailure, Page]],
      maybePerPage:  Option[ValidatedNel[ParseFailure, PerPage]],
      maybeAuthUser: Option[AuthUser]
  ): F[Response[F]] = {
    import io.renku.knowledgegraph.datasets.DatasetsSearchEndpoint.Sort
    import io.renku.knowledgegraph.datasets.DatasetsSearchEndpoint.Sort._

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
      maybeSince:   Option[ValidatedNel[ParseFailure, entities.Endpoint.Criteria.Filters.Since]],
      maybeUntil:   Option[ValidatedNel[ParseFailure, entities.Endpoint.Criteria.Filters.Until]],
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
      maybeSince.map(_.map(Option.apply)).getOrElse(Validated.validNel(Option.empty[Since])),
      maybeUntil.map(_.map(Option.apply)).getOrElse(Validated.validNel(Option.empty[Until])),
      maybeSort getOrElse Validated.validNel(Sorting.By(ByName, Direction.Asc)),
      PagingRequest(maybePage, maybePerPage),
      (maybeSince -> maybeUntil)
        .mapN(_ -> _)
        .map {
          case (Valid(since), Valid(until)) if (since.value compareTo until.value) > 0 =>
            ParseFailure("'since' parameter > 'until'", "").invalidNel[Unit]
          case _ => ().validNel[ParseFailure]
        }
        .getOrElse(().validNel[ParseFailure])
    ).mapN { case (maybeQuery, types, creators, visibilities, maybeSince, maybeUntil, sorting, paging, _) =>
      `GET /entities`(
        Criteria(Filters(maybeQuery, types, creators, visibilities, maybeSince, maybeUntil),
                 sorting,
                 paging,
                 maybeUser
        ),
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
      path:           Path,
      maybeAuthUser:  Option[AuthUser]
  )(implicit request: Request[F]): F[Response[F]] = path.segments.toList.map(_.toString) match {
    case projectPathParts :+ "datasets" =>
      projectPathParts.toProjectPath
        .flatTap(authorizePath(_, maybeAuthUser).leftMap(_.toHttpResponse))
        .semiflatMap(getProjectDatasets)
        .merge
    case projectPathParts :+ "files" :+ location :+ "lineage" =>
      getLineage(projectPathParts, location, maybeAuthUser)
    case projectPathParts =>
      projectPathParts.toProjectPath
        .flatTap(authorizePath(_, maybeAuthUser).leftMap(_.toHttpResponse))
        .semiflatMap(`GET /projects/:path`(_, maybeAuthUser))
        .merge
  }

  private def getLineage(projectPathParts: List[String], location: String, maybeAuthUser: Option[AuthUser]) = {
    import projects.files.lineage.model.Node.Location

    def toLocation(location: String): EitherT[F, Response[F], Location] = EitherT.fromEither[F] {
      Location
        .from(Uri.decode(location))
        .leftMap(_ => Response[F](Status.NotFound).withEntity(InfoMessage("Resource not found")))
    }

    (projectPathParts.toProjectPath -> toLocation(location))
      .mapN(_ -> _)
      .flatTap { case (projectPath, _) => authorizePath(projectPath, maybeAuthUser).leftMap(_.toHttpResponse) }
      .semiflatMap { case (projectPath, location) => `GET /lineage`(projectPath, location, maybeAuthUser) }
      .merge
  }

  private implicit class PathPartsOps(parts: List[String]) {
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
  ): IO[MicroserviceRoutes[IO]] = GitLabClient[IO]() >>= { implicit gitLabClient =>
    AccessTokenFinder[IO]() >>= { implicit accessTokenFinder =>
      for {
        datasetsSearchEndpoint  <- DatasetsSearchEndpoint[IO]
        datasetEndpoint         <- DatasetEndpoint[IO]
        entitiesEndpoint        <- entities.Endpoint[IO]
        queryEndpoint           <- QueryEndpoint()
        lineageEndpoint         <- projects.files.lineage.Endpoint[IO]
        ontologyEndpoint        <- ontology.Endpoint[IO]
        projectDetailsEndpoint  <- projects.details.Endpoint[IO]
        projectDatasetsEndpoint <- ProjectDatasetsEndpoint[IO]
        docsEndpoint            <- docs.Endpoint[IO]
        usersProjectsEndpoint   <- users.projects.Endpoint[IO]
        authenticator           <- GitLabAuthenticator[IO]
        authMiddleware          <- Authentication.middlewareAuthenticatingIfNeeded(authenticator)
        projectPathAuthorizer   <- Authorizer.using(ProjectPathRecordsFinder[IO])
        datasetIdAuthorizer     <- Authorizer.using(DatasetIdRecordsFinder[IO])
        versionRoutes           <- version.Routes[IO]
      } yield new MicroserviceRoutes(datasetsSearchEndpoint,
                                     datasetEndpoint,
                                     entitiesEndpoint,
                                     queryEndpoint,
                                     lineageEndpoint,
                                     ontologyEndpoint,
                                     projectDetailsEndpoint,
                                     projectDatasetsEndpoint,
                                     docsEndpoint,
                                     usersProjectsEndpoint,
                                     authMiddleware,
                                     projectPathAuthorizer,
                                     datasetIdAuthorizer,
                                     new RoutesMetrics[IO],
                                     versionRoutes
      )
    }
  }
}
