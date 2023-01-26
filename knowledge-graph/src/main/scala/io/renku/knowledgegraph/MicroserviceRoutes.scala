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

import cats.data.Validated.Valid
import cats.data.{EitherT, Validated, ValidatedNel}
import cats.effect.unsafe.IORuntime
import cats.effect.{Async, IO, Resource}
import cats.syntax.all._
import io.renku.entities.search.{Criteria => EntitiesSearchCriteria}
import io.renku.graph.http.server.security._
import io.renku.graph.model
import io.renku.graph.model.persons
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.InfoMessage
import io.renku.http.InfoMessage._
import io.renku.http.client.GitLabClient
import io.renku.http.rest.Sorting
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.PagingRequest.Decoders._
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.server.QueryParameterTools._
import io.renku.http.server.security.Authentication
import io.renku.http.server.security.model.AuthUser
import io.renku.http.server.version
import io.renku.metrics.{MetricsRegistry, RoutesMetrics}
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.http4s.dsl.Http4sDsl
import org.http4s.server.AuthMiddleware
import org.http4s.{AuthedRoutes, ParseFailure, Request, Response, Status, Uri}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private class MicroserviceRoutes[F[_]: Async](
    datasetsSearchEndpoint:     datasets.Endpoint[F],
    datasetDetailsEndpoint:     datasets.details.Endpoint[F],
    entitiesEndpoint:           entities.Endpoint[F],
    lineageEndpoint:            projects.files.lineage.Endpoint[F],
    ontologyEndpoint:           ontology.Endpoint[F],
    projectDetailsEndpoint:     projects.details.Endpoint[F],
    projectDatasetsEndpoint:    projects.datasets.Endpoint[F],
    projectDatasetTagsEndpoint: projects.datasets.tags.Endpoint[F],
    docsEndpoint:               docs.Endpoint[F],
    usersProjectsEndpoint:      users.projects.Endpoint[F],
    authMiddleware:             AuthMiddleware[F, Option[AuthUser]],
    projectPathAuthorizer:      Authorizer[F, model.projects.Path],
    datasetIdAuthorizer:        Authorizer[F, model.datasets.Identifier],
    routesMetrics:              RoutesMetrics[F],
    versionRoutes:              version.Routes[F]
) extends Http4sDsl[F] {

  import datasetDetailsEndpoint._
  import datasetIdAuthorizer.{authorize => authorizeDatasetId}
  import entitiesEndpoint._
  import lineageEndpoint._
  import ontologyEndpoint._
  import org.http4s.HttpRoutes
  import projectDatasetTagsEndpoint._
  import projectDatasetsEndpoint._
  import projectDetailsEndpoint._
  import projectPathAuthorizer.{authorize => authorizePath}
  import routesMetrics._

  lazy val routes: Resource[F, HttpRoutes[F]] =
    (versionRoutes() <+> nonAuthorizedRoutes <+> authorizedRoutes).withMetrics

  private lazy val authorizedRoutes: HttpRoutes[F] = authMiddleware {
    `GET /datasets/*` <+> `GET /entities/*` <+> `GET /projects/*` <+> `GET /users/*`
  }

  private lazy val `GET /datasets/*` : AuthedRoutes[Option[AuthUser], F] = {
    import datasets.Endpoint.Query.query
    import datasets.Endpoint.Sort.sort
    import datasets._

    AuthedRoutes.of {
      case GET -> Root / "knowledge-graph" / "datasets"
          :? query(maybePhrase) +& sort(sortBy) +& page(page) +& perPage(perPage) as maybeUser =>
        searchForDatasets(maybePhrase, sortBy, page, perPage, maybeUser)
      case GET -> Root / "knowledge-graph" / "datasets" / DatasetId(id) as maybeUser => fetchDataset(id, maybeUser)
    }
  }

  // format: off
  private lazy val `GET /entities/*`: AuthedRoutes[Option[AuthUser], F] = {
    import io.renku.entities.search.Criteria._
    import Sort.sort
    import entities.QueryParamDecoders._

    AuthedRoutes.of {
      case req@GET -> Root / "knowledge-graph" / "entities"
        :? query(maybeQuery) +& entityTypes(maybeTypes) +& creatorNames(maybeCreators)
        +& visibilities(maybeVisibilities) +& namespaces(maybeNamespaces) 
        +& since(maybeSince) +& until(maybeUntil) 
        +& sort(maybeSort) +& page(maybePage) +& perPage(maybePerPage) as maybeUser =>
        searchForEntities(maybeQuery, maybeTypes, maybeCreators, maybeVisibilities, maybeNamespaces,
          maybeSince, maybeUntil, maybeSort, maybePage, maybePerPage, maybeUser, req.req)
    }
  }
  // format: on

  private lazy val `GET /users/*` : AuthedRoutes[Option[AuthUser], F] = {
    import users.binders._
    import users.projects.Endpoint.Criteria.Filters
    import users.projects.Endpoint.Criteria.Filters.ActivationState._
    import users.projects.Endpoint.Criteria.Filters._
    import users.projects.Endpoint._
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

  private lazy val `GET /projects/*` : AuthedRoutes[Option[AuthUser], F] = AuthedRoutes.of {
    case authReq @ GET -> "knowledge-graph" /: "projects" /: path as maybeUser =>
      routeToProjectsEndpoints(path, maybeUser)(authReq.req)
  }

  private lazy val nonAuthorizedRoutes: HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ GET -> "knowledge-graph" /: "ontology" /: path => `GET /ontology`(path)(req)
    case GET -> Root / "knowledge-graph" / "spec.json"        => docsEndpoint.`get /spec.json`
    case GET -> Root / "ping"                                 => Ok("pong")
  }

  private def searchForDatasets(
      maybePhrase:   Option[ValidatedNel[ParseFailure, datasets.Endpoint.Query.Phrase]],
      maybeSort:     ValidatedNel[ParseFailure, List[datasets.Endpoint.Sort.By]],
      maybePage:     Option[ValidatedNel[ParseFailure, Page]],
      maybePerPage:  Option[ValidatedNel[ParseFailure, PerPage]],
      maybeAuthUser: Option[AuthUser]
  ): F[Response[F]] = {
    import datasets.Endpoint.Query._
    import datasets.Endpoint.Sort

    (maybePhrase.map(_.map(Option.apply)).getOrElse(Validated.validNel(Option.empty[Phrase])),
     maybeSort,
     PagingRequest(maybePage, maybePerPage)
    ).mapN { case (maybePhrase, sort, paging) =>
      val sortOrDefault = Sorting.fromList(sort).getOrElse(Sort.default)
      datasetsSearchEndpoint.searchForDatasets(maybePhrase, sortOrDefault, paging, maybeAuthUser)
    }.fold(toBadRequest, identity)
  }

  private def searchForEntities(
      maybeQuery:   Option[ValidatedNel[ParseFailure, EntitiesSearchCriteria.Filters.Query]],
      types:        ValidatedNel[ParseFailure, List[EntitiesSearchCriteria.Filters.EntityType]],
      creators:     ValidatedNel[ParseFailure, List[persons.Name]],
      visibilities: ValidatedNel[ParseFailure, List[model.projects.Visibility]],
      namespaces:   ValidatedNel[ParseFailure, List[model.projects.Namespace]],
      maybeSince:   Option[ValidatedNel[ParseFailure, EntitiesSearchCriteria.Filters.Since]],
      maybeUntil:   Option[ValidatedNel[ParseFailure, EntitiesSearchCriteria.Filters.Until]],
      sorting:      ValidatedNel[ParseFailure, List[EntitiesSearchCriteria.Sort.By]],
      maybePage:    Option[ValidatedNel[ParseFailure, Page]],
      maybePerPage: Option[ValidatedNel[ParseFailure, PerPage]],
      maybeUser:    Option[AuthUser],
      request:      Request[F]
  ): F[Response[F]] = {
    import EntitiesSearchCriteria.Filters._
    import EntitiesSearchCriteria.{Filters, Sort}
    (
      maybeQuery.map(_.map(Option.apply)).getOrElse(Validated.validNel(Option.empty[Query])),
      types.map(_.toSet),
      creators.map(_.toSet),
      visibilities.map(_.toSet),
      namespaces.map(_.toSet),
      maybeSince.map(_.map(Option.apply)).getOrElse(Validated.validNel(Option.empty[Since])),
      maybeUntil.map(_.map(Option.apply)).getOrElse(Validated.validNel(Option.empty[Until])),
      sorting.map(Sorting.fromListOrDefault(_, Sort.default)),
      PagingRequest(maybePage, maybePerPage),
      (maybeSince -> maybeUntil)
        .mapN(_ -> _)
        .map {
          case (Valid(since), Valid(until)) if (since.value compareTo until.value) > 0 =>
            ParseFailure("'since' parameter > 'until'", "").invalidNel[Unit]
          case _ => ().validNel[ParseFailure]
        }
        .getOrElse(().validNel[ParseFailure])
    ).mapN { case (maybeQuery, types, creators, visibilities, namespaces, maybeSince, maybeUntil, sorting, paging, _) =>
      `GET /entities`(
        EntitiesSearchCriteria(Filters(maybeQuery, types, creators, visibilities, namespaces, maybeSince, maybeUntil),
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
      path:          Path,
      maybeAuthUser: Option[AuthUser]
  )(implicit request: Request[F]): F[Response[F]] = path.segments.toList.map(_.toString) match {
    case projectPathParts :+ "datasets" :+ datasets.DatasetName(dsName) :+ "tags" =>
      import projects.datasets.tags.Endpoint._

      PagingRequest(page.find(request.uri.query), perPage.find(request.uri.query))
        .map(paging =>
          projectPathParts.toProjectPath
            .flatTap(authorizePath(_, maybeAuthUser).leftMap(_.toHttpResponse))
            .semiflatMap(path =>
              `GET /projects/:path/datasets/:name/tags`(Criteria(path, dsName, paging, maybeAuthUser))
            )
            .merge
        )
        .fold(toBadRequest, identity)

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
  ): IO[MicroserviceRoutes[IO]] = for {
    implicit0(gv: GitLabClient[IO])       <- GitLabClient[IO]()
    implicit0(atf: AccessTokenFinder[IO]) <- AccessTokenFinder[IO]()
    datasetsSearchEndpoint                <- datasets.Endpoint[IO]
    datasetDetailsEndpoint                <- datasets.details.Endpoint[IO]
    entitiesEndpoint                      <- entities.Endpoint[IO]
    lineageEndpoint                       <- projects.files.lineage.Endpoint[IO]
    ontologyEndpoint                      <- ontology.Endpoint[IO]
    projectDetailsEndpoint                <- projects.details.Endpoint[IO]
    projectDatasetsEndpoint               <- projects.datasets.Endpoint[IO]
    projectDatasetTagsEndpoint            <- projects.datasets.tags.Endpoint[IO]
    docsEndpoint                          <- docs.Endpoint[IO]
    usersProjectsEndpoint                 <- users.projects.Endpoint[IO]
    authenticator                         <- GitLabAuthenticator[IO]
    authMiddleware                        <- Authentication.middlewareAuthenticatingIfNeeded(authenticator)
    projectPathAuthorizer                 <- Authorizer.using(ProjectPathRecordsFinder[IO])
    datasetIdAuthorizer                   <- Authorizer.using(DatasetIdRecordsFinder[IO])
    versionRoutes                         <- version.Routes[IO]
  } yield new MicroserviceRoutes(
    datasetsSearchEndpoint,
    datasetDetailsEndpoint,
    entitiesEndpoint,
    lineageEndpoint,
    ontologyEndpoint,
    projectDetailsEndpoint,
    projectDatasetsEndpoint,
    projectDatasetTagsEndpoint,
    docsEndpoint,
    usersProjectsEndpoint,
    authMiddleware,
    projectPathAuthorizer,
    datasetIdAuthorizer,
    new RoutesMetrics[IO],
    versionRoutes
  )
}
