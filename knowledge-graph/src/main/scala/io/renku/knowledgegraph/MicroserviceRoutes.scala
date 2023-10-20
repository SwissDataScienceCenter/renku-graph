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

import QueryParamDecoders._
import cats.data.Validated.Valid
import cats.data.{EitherT, Validated, ValidatedNel}
import cats.effect.{Async, Resource}
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import com.typesafe.config.ConfigFactory
import eu.timepit.refined.auto._
import io.renku.data.Message
import io.renku.entities.search.{Criteria => EntitiesSearchCriteria}
import io.renku.entities.viewings.search.RecentEntitiesFinder
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.http.server.security._
import io.renku.graph.model
import io.renku.graph.model.{RenkuUrl, persons}
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.http.rest.Sorting
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.PagingRequest.Decoders._
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.server.QueryParameterTools._
import io.renku.http.server.security.Authentication
import io.renku.http.server.security.model.{AuthUser, MaybeAuthUser}
import io.renku.http.server.version
import io.renku.knowledgegraph.datasets.details.RequestedDataset
import io.renku.knowledgegraph.datasets.{DatasetIdRecordsFinder, DatasetSameAsRecordsFinder}
import io.renku.metrics.{MetricsRegistry, RoutesMetrics}
import io.renku.triplesstore.{ProjectSparqlClient, ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.http4s.dsl.Http4sDsl
import org.http4s.server.AuthMiddleware
import org.http4s.{AuthedRoutes, ParseFailure, Request, Response, Status, Uri}
import org.typelevel.log4cats.Logger

private class MicroserviceRoutes[F[_]: Async](
    datasetsSearchEndpoint:     datasets.Endpoint[F],
    datasetDetailsEndpoint:     datasets.details.Endpoint[F],
    entitiesEndpoint:           entities.Endpoint[F],
    ontologyEndpoint:           ontology.Endpoint[F],
    projectDeleteEndpoint:      projects.delete.Endpoint[F],
    projectDetailsEndpoint:     projects.details.Endpoint[F],
    projectCreateEndpoint:      projects.create.Endpoint[F],
    projectUpdateEndpoint:      projects.update.Endpoint[F],
    projectDatasetsEndpoint:    projects.datasets.Endpoint[F],
    projectDatasetTagsEndpoint: projects.datasets.tags.Endpoint[F],
    recentEntitiesEndpoint:     entities.currentuser.recentlyviewed.Endpoint[F],
    lineageEndpoint:            projects.files.lineage.Endpoint[F],
    docsEndpoint:               docs.Endpoint[F],
    usersProjectsEndpoint:      users.projects.Endpoint[F],
    authMiddleware:             AuthMiddleware[F, MaybeAuthUser],
    projectSlugAuthorizer:      Authorizer[F, model.projects.Slug],
    datasetIdAuthorizer:        Authorizer[F, model.datasets.Identifier],
    datasetSameAsAuthorizer:    Authorizer[F, model.datasets.SameAs],
    routesMetrics:              RoutesMetrics[F],
    versionRoutes:              version.Routes[F]
)(implicit ru: RenkuUrl)
    extends Http4sDsl[F] {

  import datasetDetailsEndpoint._
  import datasetIdAuthorizer.{authorize => authorizeDatasetId}
  import datasetSameAsAuthorizer.{authorize => authorizeDatasetSameAs}
  import entitiesEndpoint._
  import lineageEndpoint._
  import ontologyEndpoint._
  import org.http4s.HttpRoutes
  import projectCreateEndpoint._
  import projectDatasetTagsEndpoint._
  import projectDeleteEndpoint._
  import projectDetailsEndpoint._
  import projectSlugAuthorizer.{authorize => authorizeSlug}
  import projectUpdateEndpoint._
  import routesMetrics._

  lazy val routes: Resource[F, HttpRoutes[F]] =
    (versionRoutes() <+>
      versionRoutes.on(Root / "knowledge-graph" / "version") <+>
      nonAuthorizedRoutes <+> authorizedRoutes).withMetrics

  private lazy val authorizedRoutes: HttpRoutes[F] = authMiddleware {
    `GET /datasets/*` <+> `GET /entities/*` <+> `GET /projects/*` <+> `GET /users/*` <+> getRecentEntities
  }

  private lazy val getRecentEntities: AuthedRoutes[MaybeAuthUser, F] =
    AuthedRoutes.of {
      case GET -> Root / "knowledge-graph" / "current-user" / "recently-viewed" :?
          recentlyViewedEntityTypes(types) +& LimitQueryParam(limit) as maybeUser =>
        maybeUser.withAuthenticatedUser { user =>
          val defaultLimit = Validated.validNel[ParseFailure, Int](10)
          val response =
            (types, limit.getOrElse(defaultLimit)).mapN { (ets, count) =>
              val criteria = RecentEntitiesFinder.Criteria(ets.toSet, user, count)
              recentEntitiesEndpoint.getRecentlyViewedEntities(criteria)
            }
          response.fold(toBadRequest, identity)
        }
    }

  private lazy val `GET /datasets/*` : AuthedRoutes[MaybeAuthUser, F] = {
    import datasets.Endpoint.Query.query
    import datasets.Endpoint.Sort.sort

    AuthedRoutes.of {
      case GET -> Root / "knowledge-graph" / "datasets"
          :? query(maybePhrase) +& sort(sortBy) +& page(page) +& perPage(perPage) as maybeUser =>
        searchForDatasets(maybePhrase, sortBy, page, perPage, maybeUser.option)
      case GET -> Root / "knowledge-graph" / "datasets" / RequestedDataset(dsId) as maybeUser =>
        fetchDataset(dsId, maybeUser.option)
    }
  }

  // format: off
  private lazy val `GET /entities/*`: AuthedRoutes[MaybeAuthUser, F] = {
    import io.renku.entities.search.Criteria._
    import Sort.sort

    AuthedRoutes.of {
      case req@GET -> Root / "knowledge-graph" / "entities"
        :? query(maybeQuery) +& entityTypes(maybeTypes) +& creatorNames(maybeCreators)
        +& visibilities(maybeVisibilities) +& namespaces(maybeNamespaces) 
        +& since(maybeSince) +& until(maybeUntil) 
        +& sort(maybeSort) +& page(maybePage) +& perPage(maybePerPage) as maybeUser =>
        searchForEntities(maybeQuery, maybeTypes, maybeCreators, maybeVisibilities, maybeNamespaces,
          maybeSince, maybeUntil, maybeSort, maybePage, maybePerPage, maybeUser.option, req.req)
    }
  }
  // format: on

  private lazy val `GET /users/*` : AuthedRoutes[MaybeAuthUser, F] = {
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
            `GET /users/:id/projects`(Criteria(userId = id, Filters(activationState), paging, maybeUser.option),
                                      req.req
            )
          }
          .fold(toBadRequest, identity)
    }
  }

  private lazy val `GET /projects/*` : AuthedRoutes[MaybeAuthUser, F] = AuthedRoutes.of {

    case authReq @ POST -> Root / "knowledge-graph" / "projects" as maybeUser =>
      maybeUser.withUserOrNotFound {
        `POST /projects`(authReq.req, _)
      }

    case DELETE -> "knowledge-graph" /: "projects" /: path as maybeUser =>
      maybeUser.withUserOrNotFound { user =>
        path.segments.toList
          .map(_.toString)
          .toProjectSlug
          .flatTap(authorizeSlug(_, user.some).leftMap(_.toHttpResponse))
          .semiflatMap(`DELETE /projects/:slug`(_, user))
          .merge
      }

    case authReq @ GET -> "knowledge-graph" /: "projects" /: path as maybeUser =>
      routeToProjectsEndpoints(path, maybeUser.option)(authReq.req)

    case authReq @ (PATCH | PUT) -> "knowledge-graph" /: "projects" /: path as maybeUser =>
      maybeUser.withUserOrNotFound { user =>
        path.segments.toList
          .map(_.toString)
          .toProjectSlug
          .flatTap(authorizeSlug(_, user.some).leftMap(_.toHttpResponse))
          .semiflatMap(`PATCH /projects/:slug`(_, authReq.req, user))
          .merge
      }
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

  private def fetchDataset(requestedDS: RequestedDataset, maybeAuthUser: Option[AuthUser]): F[Response[F]] =
    requestedDS
      .fold(
        authorizeDatasetId(_, maybeAuthUser).map(_.replaceKey(requestedDS)),
        authorizeDatasetSameAs(_, maybeAuthUser).map(_.replaceKey(requestedDS))
      )
      .leftMap(_.toHttpResponse[F])
      .semiflatMap(`GET /datasets/:id`(requestedDS, _))
      .merge

  private def routeToProjectsEndpoints(
      path:          Path,
      maybeAuthUser: Option[AuthUser]
  )(implicit request: Request[F]): F[Response[F]] = path.segments.toList.map(_.toString) match {
    case projectSlugParts :+ "datasets" :+ datasets.DatasetSlug(dsSlug) :+ "tags" =>
      import projects.datasets.tags.Endpoint._

      PagingRequest(page.find(request.uri.query), perPage.find(request.uri.query))
        .map(paging =>
          projectSlugParts.toProjectSlug
            .flatTap(authorizeSlug(_, maybeAuthUser).leftMap(_.toHttpResponse))
            .semiflatMap(slug =>
              `GET /projects/:slug/datasets/:dsSlug/tags`(Criteria(slug, dsSlug, paging, maybeAuthUser))
            )
            .merge
        )
        .fold(toBadRequest, identity)

    case projectSlugParts :+ "datasets" =>
      import projects.datasets.Endpoint.Criteria
      import Criteria.Sort
      import Criteria.Sort.sort

      (
        sort.find(request.uri.query).sequence,
        PagingRequest(page.find(request.uri.query), perPage.find(request.uri.query))
      ).mapN { (maybeSorts, paging) =>
        val sorting: Sorting[Criteria.Sort.type] = Sorting.fromOptionalListOrDefault(maybeSorts, Sort.default)
        projectSlugParts.toProjectSlug
          .flatTap(authorizeSlug(_, maybeAuthUser).leftMap(_.toHttpResponse))
          .semiflatMap(slug =>
            projectDatasetsEndpoint.`GET /projects/:slug/datasets`(request, Criteria(slug, sorting, paging))
          )
          .merge
      }.fold(toBadRequest, identity)
    case projectSlugParts :+ "files" :+ location :+ "lineage" =>
      getLineage(projectSlugParts, location, maybeAuthUser)
    case projectSlugParts =>
      projectSlugParts.toProjectSlug
        .flatTap(authorizeSlug(_, maybeAuthUser).leftMap(_.toHttpResponse))
        .semiflatMap(`GET /projects/:slug`(_, maybeAuthUser))
        .merge
  }

  private def getLineage(projectSlugParts: List[String], location: String, maybeAuthUser: Option[AuthUser]) = {
    import projects.files.lineage.model.Node.Location

    def toLocation(location: String): EitherT[F, Response[F], Location] = EitherT.fromEither[F] {
      Location
        .from(Uri.decode(location))
        .leftMap(_ => Response[F](Status.NotFound).withEntity(Message.Info("Resource not found")))
    }

    (projectSlugParts.toProjectSlug -> toLocation(location))
      .mapN(_ -> _)
      .flatTap { case (projectSlug, _) => authorizeSlug(projectSlug, maybeAuthUser).leftMap(_.toHttpResponse) }
      .semiflatMap { case (projectSlug, location) => `GET /lineage`(projectSlug, location, maybeAuthUser) }
      .merge
  }

  private implicit class PathPartsOps(parts: List[String]) {
    lazy val toProjectSlug: EitherT[F, Response[F], model.projects.Slug] = EitherT.fromEither[F] {
      model.projects.Slug
        .from(parts mkString "/")
        .leftMap(_ => Response[F](Status.NotFound).withEntity(Message.Info("Resource not found")))
    }
  }
}

private object MicroserviceRoutes {

  def apply[F[_]: Async: Parallel: Logger: MetricsRegistry: SparqlQueryTimeRecorder](
      projectConnConfig:   ProjectsConnectionConfig,
      projectSparqlClient: ProjectSparqlClient[F]
  ): F[MicroserviceRoutes[F]] = for {
    config                               <- MonadThrow[F].catchNonFatal(ConfigFactory.load())
    implicit0(gv: GitLabClient[F])       <- GitLabClient[F]()
    implicit0(atf: AccessTokenFinder[F]) <- AccessTokenFinder[F]()
    implicit0(ru: RenkuUrl)              <- RenkuUrlLoader[F]()

    datasetsSearchEndpoint     <- datasets.Endpoint[F]
    datasetDetailsEndpoint     <- datasets.details.Endpoint[F]
    entitiesEndpoint           <- entities.Endpoint[F]
    recentEntitiesEndpoint     <- entities.currentuser.recentlyviewed.Endpoint[F](projectConnConfig)
    ontologyEndpoint           <- ontology.Endpoint[F]
    projectDeleteEndpoint      <- projects.delete.Endpoint[F]
    projectDetailsEndpoint     <- projects.details.Endpoint[F]
    projectCreateEndpoint      <- projects.create.Endpoint[F](config)
    projectUpdateEndpoint      <- projects.update.Endpoint[F]
    projectDatasetsEndpoint    <- projects.datasets.Endpoint[F]
    projectDatasetTagsEndpoint <- projects.datasets.tags.Endpoint[F]
    lineageEndpoint            <- projects.files.lineage.Endpoint[F]
    docsEndpoint               <- docs.Endpoint[F]
    usersProjectsEndpoint      <- users.projects.Endpoint[F]
    authenticator              <- GitLabAuthenticator[F]
    authMiddleware             <- Authentication.middlewareAuthenticatingIfNeeded(authenticator)
    projectSlugAuthorizer      <- ProjectSlugRecordsFinder[F](projectSparqlClient, ru).map(_.asAuthorizer)
    datasetIdAuthorizer     = DatasetIdRecordsFinder[F](projectSparqlClient).asAuthorizer
    datasetSameAsAuthorizer = DatasetSameAsRecordsFinder[F](projectSparqlClient).asAuthorizer
    versionRoutes <- version.Routes[F]
  } yield new MicroserviceRoutes(
    datasetsSearchEndpoint,
    datasetDetailsEndpoint,
    entitiesEndpoint,
    ontologyEndpoint,
    projectDeleteEndpoint,
    projectDetailsEndpoint,
    projectCreateEndpoint,
    projectUpdateEndpoint,
    projectDatasetsEndpoint,
    projectDatasetTagsEndpoint,
    recentEntitiesEndpoint,
    lineageEndpoint,
    docsEndpoint,
    usersProjectsEndpoint,
    authMiddleware,
    projectSlugAuthorizer,
    datasetIdAuthorizer,
    datasetSameAsAuthorizer,
    new RoutesMetrics[F],
    versionRoutes
  )
}
