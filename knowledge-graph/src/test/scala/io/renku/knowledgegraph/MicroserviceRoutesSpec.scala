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

import cats.data.{Kleisli, OptionT}
import cats.data.EitherT.{leftT, rightT}
import cats.effect.{IO, Resource}
import cats.syntax.all._
import io.circe.Json
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.http.server.security.Authorizer
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model
import io.renku.graph.model.GraphModelGenerators._
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.http.ErrorMessage.ErrorMessage
import io.renku.http.InfoMessage._
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.http.rest.{SortBy, Sorting}
import io.renku.http.rest.SortBy.Direction
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.server.EndpointTester._
import io.renku.http.server.security.EndpointSecurityException
import io.renku.http.server.security.EndpointSecurityException.AuthorizationFailure
import io.renku.http.server.security.model.AuthUser
import io.renku.http.server.version
import io.renku.interpreters.TestRoutesMetrics
import io.renku.testtools.IOSpec
import org.http4s._
import org.http4s.MediaType.application
import org.http4s.Method.{DELETE, GET}
import org.http4s.Status._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.http4s.server.AuthMiddleware
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import projects.files.lineage.LineageGenerators._
import projects.files.lineage.model.Node.Location

import scala.language.reflectiveCalls

class MicroserviceRoutesSpec
    extends AnyWordSpec
    with MockFactory
    with ScalaCheckPropertyChecks
    with should.Matchers
    with IOSpec {

  "GET /knowledge-graph/datasets?query=<phrase>" should {

    import datasets._
    import datasets.Endpoint.Query._
    import datasets.Endpoint.Sort
    import datasets.Endpoint.Sort._

    s"return $Ok when a valid 'query' and no 'sort', `page` and `per_page` parameters given" in new TestCase {

      val maybeAuthUser = authUsers.generateOption
      val phrase        = nonEmptyStrings().generateOne
      (datasetsSearchEndpoint
        .searchForDatasets(_: Option[Phrase], _: Sorting[Sort.type], _: PagingRequest, _: Option[AuthUser]))
        .expects(Phrase(phrase).some,
                 Sorting(Sort.By(TitleProperty, Direction.Asc)),
                 PagingRequest(Page.first, PerPage.default),
                 maybeAuthUser
        )
        .returning(IO.pure(Response[IO](Ok)))

      routes(maybeAuthUser)
        .call(Request[IO](GET, uri"/knowledge-graph/datasets".withQueryParam(query.parameterName, phrase)))
        .status shouldBe Ok
    }

    s"return $Ok when no ${query.parameterName} parameter given" in new TestCase {

      val maybeAuthUser = authUsers.generateOption

      (datasetsSearchEndpoint
        .searchForDatasets(_: Option[Phrase], _: Sorting[Sort.type], _: PagingRequest, _: Option[AuthUser]))
        .expects(Option.empty[Phrase],
                 Sorting(Sort.By(TitleProperty, Direction.Asc)),
                 PagingRequest(Page.first, PerPage.default),
                 maybeAuthUser
        )
        .returning(IO.pure(Response[IO](Ok)))

      routes(maybeAuthUser).call(Request[IO](GET, uri"/knowledge-graph/datasets")).status shouldBe Ok
    }

    s"return $Unauthorized when user authentication failed" in new TestCase {
      routes(givenAuthFailing())
        .call(Request[IO](GET, uri"/knowledge-graph/datasets"))
        .status shouldBe Unauthorized
    }

    Sort.properties foreach { sortProperty =>
      val sortBy = Sort.By(sortProperty, Gen.oneOf(SortBy.Direction.Asc, SortBy.Direction.Desc).generateOne)

      s"return $Ok when '${query.parameterName}' and 'sort=${sortBy.property}:${sortBy.direction}' parameters given" in new TestCase {
        val maybeAuthUser = authUsers.generateOption

        val phrase = phrases.generateOne
        val request = Request[IO](
          GET,
          uri"/knowledge-graph/datasets"
            .withQueryParam("query", phrase.value)
            .withQueryParam("sort", s"${sortBy.property}:${sortBy.direction}")
        )
        (datasetsSearchEndpoint
          .searchForDatasets(_: Option[Phrase], _: Sorting[Sort.type], _: PagingRequest, _: Option[AuthUser]))
          .expects(phrase.some, Sorting(sortBy), PagingRequest.default, maybeAuthUser)
          .returning(IO.pure(Response[IO](Ok)))

        val response = routes(maybeAuthUser).call(request)

        response.status shouldBe Ok

        routesMetrics.clearRegistry()
      }
    }

    s"return $Ok when query, ${PagingRequest.Decoders.page.parameterName} and ${PagingRequest.Decoders.perPage.parameterName} parameters given" in new TestCase {
      forAll(phrases, pages, perPages) { (phrase, page, perPage) =>
        val maybeAuthUser = authUsers.generateOption

        val request = Request[IO](
          GET,
          uri"/knowledge-graph/datasets"
            .withQueryParam("query", phrase.value)
            .withQueryParam("page", page.value)
            .withQueryParam("per_page", perPage.value)
        )
        (datasetsSearchEndpoint
          .searchForDatasets(_: Option[Phrase], _: Sorting[Sort.type], _: PagingRequest, _: Option[AuthUser]))
          .expects(phrase.some,
                   Sorting(Sort.By(TitleProperty, Direction.Asc)),
                   PagingRequest(page, perPage),
                   maybeAuthUser
          )
          .returning(IO.pure(Response[IO](Ok)))

        routes(maybeAuthUser).call(request).status shouldBe Ok

        routesMetrics.clearRegistry()
      }
    }

    s"return $BadRequest for invalid " +
      s"${query.parameterName} parameter, " +
      s"${Sort.sort.parameterName} parameter, " +
      s"${PagingRequest.Decoders.page.parameterName} parameter and " +
      s"${PagingRequest.Decoders.perPage.parameterName} parameter" in new TestCase {

        val sortProperty     = "invalid"
        val requestedPage    = nonPositiveInts().generateOne
        val requestedPerPage = nonPositiveInts().generateOne

        val response = routes().call {
          Request(
            GET,
            uri"/knowledge-graph/datasets"
              .withQueryParam("query", blankStrings().generateOne)
              .withQueryParam("sort", s"$sortProperty:${Direction.Asc}")
              .withQueryParam("page", requestedPage.toString)
              .withQueryParam("per_page", requestedPerPage.toString)
          )
        }

        response.status shouldBe BadRequest
        response.body[ErrorMessage] shouldBe ErrorMessage(
          List(
            s"'${query.parameterName}' parameter with invalid value",
            Sort.sort.errorMessage(sortProperty),
            PagingRequest.Decoders.page.errorMessage(requestedPage.value.toString),
            PagingRequest.Decoders.perPage.errorMessage(requestedPerPage.value.toString)
          ).mkString("; ")
        )
      }
  }

  "GET /knowledge-graph/datasets/:id" should {

    s"return $Ok when a valid :id path parameter given" in new TestCase {
      val id            = datasetIdentifiers.generateOne
      val maybeAuthUser = authUsers.generateOption

      val authContext = AuthContext(maybeAuthUser, id, projectPaths.generateSet())
      (datasetIdAuthorizer.authorize _)
        .expects(id, maybeAuthUser)
        .returning(rightT[IO, EndpointSecurityException](authContext))

      (datasetDetailsEndpoint.getDataset _).expects(id, authContext).returning(IO.pure(Response[IO](Ok)))

      routes(maybeAuthUser)
        .call(Request(GET, uri"/knowledge-graph/datasets" / id.value))
        .status shouldBe Ok
    }

    s"return $NotFound when no :id path parameter given" in new TestCase {
      routes()
        .call(Request(GET, uri"/knowledge-graph/datasets/"))
        .status shouldBe NotFound
    }

    s"return $Unauthorized when user authentication failed" in new TestCase {
      routes(givenAuthIfNeededMiddleware(returning = OptionT.none[IO, Option[AuthUser]]))
        .call(Request(GET, uri"/knowledge-graph/datasets" / datasetIdentifiers.generateOne.value))
        .status shouldBe Unauthorized
    }

    s"return $NotFound when user has no rights for the project the dataset belongs to" in new TestCase {
      val id            = datasetIdentifiers.generateOne
      val maybeAuthUser = authUsers.generateOption

      (datasetIdAuthorizer.authorize _)
        .expects(id, maybeAuthUser)
        .returning(leftT[IO, AuthContext[model.datasets.Identifier]](AuthorizationFailure))

      val response = routes(maybeAuthUser).call(Request(GET, uri"/knowledge-graph/datasets" / id.show))

      response.status             shouldBe NotFound
      response.contentType        shouldBe Some(`Content-Type`(application.json))
      response.body[ErrorMessage] shouldBe InfoMessage(AuthorizationFailure.getMessage)
    }
  }

  "GET /knowledge-graph/entities" should {
    import io.renku.entities.search.Criteria
    import io.renku.entities.search.Criteria._
    import io.renku.entities.search.Criteria.Sort._
    import io.renku.entities.search.Generators._

    forAll {
      Table(
        "uri"                          -> "criteria",
        uri"/knowledge-graph/entities" -> Criteria(),
        queryParams
          .map(query =>
            uri"/knowledge-graph/entities" +? ("query" -> query.value) -> Criteria(Filters(maybeQuery = query.some))
          )
          .generateOne,
        typeParams
          .map(t => uri"/knowledge-graph/entities" +? ("type" -> t.value) -> Criteria(Filters(entityTypes = Set(t))))
          .generateOne,
        typeParams
          .toGeneratorOfList(min = 2)
          .map { list =>
            val uri = uri"/knowledge-graph/entities" ++? ("type" -> list.map(_.show))
            uri -> Criteria(Filters(entityTypes = list.toSet))
          }
          .generateOne,
        personNames
          .map(name =>
            uri"/knowledge-graph/entities" +? ("creator" -> name.value) -> Criteria(Filters(creators = Set(name)))
          )
          .generateOne,
        personNames
          .toGeneratorOfList(min = 2)
          .map { list =>
            val uri = uri"/knowledge-graph/entities" ++? ("creator" -> list.map(_.show))
            uri -> Criteria(Filters(creators = list.toSet))
          }
          .generateOne,
        projectVisibilities
          .map(v =>
            uri"/knowledge-graph/entities" +? ("visibility" -> v.value) -> Criteria(Filters(visibilities = Set(v)))
          )
          .generateOne,
        projectVisibilities
          .toGeneratorOfList(min = 2)
          .map { list =>
            val uri = uri"/knowledge-graph/entities" ++? ("visibility" -> list.map(_.show))
            uri -> Criteria(Filters(visibilities = list.toSet))
          }
          .generateOne,
        projectNamespaces
          .map(v =>
            uri"/knowledge-graph/entities" +? ("namespace" -> v.value) -> Criteria(Filters(namespaces = Set(v)))
          )
          .generateOne,
        projectNamespaces
          .toGeneratorOfList(min = 2)
          .map { list =>
            val uri = uri"/knowledge-graph/entities" ++? ("namespace" -> list.map(_.show))
            uri -> Criteria(Filters(namespaces = list.toSet))
          }
          .generateOne,
        sinceParams
          .map(d =>
            uri"/knowledge-graph/entities" +? ("since" -> d.value.toString) -> Criteria(Filters(maybeSince = d.some))
          )
          .generateOne,
        untilParams
          .map(d =>
            uri"/knowledge-graph/entities" +? ("until" -> d.value.toString) -> Criteria(Filters(maybeUntil = d.some))
          )
          .generateOne,
        sortingDirections
          .map(dir =>
            uri"/knowledge-graph/entities" +? ("sort" -> s"matchingScore:$dir") -> Criteria(sorting =
              Sorting(Sort.By(Sort.ByMatchingScore, dir))
            )
          )
          .generateOne,
        sortingDirections
          .map(dir =>
            uri"/knowledge-graph/entities" +? ("sort" -> s"name:$dir") -> Criteria(sorting =
              Sorting(Sort.By(ByName, dir))
            )
          )
          .generateOne,
        sortingDirections
          .map(dir =>
            uri"/knowledge-graph/entities" +? ("sort" -> s"date:$dir") -> Criteria(sorting =
              Sorting(Sort.By(ByDate, dir))
            )
          )
          .generateOne,
        sortingDirections
          .map(dir =>
            uri"/knowledge-graph/entities" ++? ("sort" -> Seq(s"date:$dir", s"name:$dir")) -> Criteria(
              sorting = Sorting(Sort.By(ByDate, dir), Sort.By(ByName, dir))
            )
          )
          .generateOne,
        pages
          .map(page =>
            uri"/knowledge-graph/entities" +? ("page" -> page.show) -> Criteria(paging =
              PagingRequest.default.copy(page = page)
            )
          )
          .generateOne,
        perPages
          .map(perPage =>
            uri"/knowledge-graph/entities" +? ("per_page" -> perPage.show) -> Criteria(paging =
              PagingRequest.default.copy(perPage = perPage)
            )
          )
          .generateOne
      )
    } { (uri, criteria) =>
      s"read the query parameters from $uri, pass them to the endpoint and return received response" in new TestCase {
        val request = Request[IO](GET, uri)

        val responseBody = jsons.generateOne
        (entitiesEndpoint.`GET /entities` _)
          .expects(criteria, request)
          .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

        val response = routes().call(request)

        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(application.json))
        response.body[Json]  shouldBe responseBody
      }
    }

    s"return $BadRequest for invalid parameter values" in new TestCase {
      val response = routes()
        .call(
          Request[IO](GET, uri"/knowledge-graph/entities" +? ("visibility" -> nonEmptyStrings().generateOne))
        )

      response.status             shouldBe BadRequest
      response.contentType        shouldBe Some(`Content-Type`(application.json))
      response.body[ErrorMessage] shouldBe ErrorMessage("'visibility' parameter with invalid value")
    }

    s"return $BadRequest for if since > until" in new TestCase {
      val since = sinceParams.generateOne
      val until = localDates(max = since.value.minusDays(1)).generateAs(Filters.Until)
      val response = routes().call {
        Request[IO](GET, uri"/knowledge-graph/entities" +? ("since" -> since.show) +? ("until" -> until.show))
      }

      response.status             shouldBe BadRequest
      response.contentType        shouldBe Some(`Content-Type`(application.json))
      response.body[ErrorMessage] shouldBe ErrorMessage("'since' parameter > 'until'")
    }

    "authenticate user from the request if given" in new TestCase {
      val maybeAuthUser = authUsers.generateOption
      val request       = Request[IO](GET, uri"/knowledge-graph/entities")

      val responseBody = jsons.generateOne
      (entitiesEndpoint.`GET /entities` _)
        .expects(Criteria(maybeUser = maybeAuthUser), request)
        .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

      routes(maybeAuthUser).call(request).status shouldBe Ok
    }

    s"return $Unauthorized when user authentication fails" in new TestCase {
      routes(givenAuthFailing())
        .call(Request[IO](GET, uri"/knowledge-graph/entities"))
        .status shouldBe Unauthorized
    }
  }

  "GET /knowledge-graph/ontology" should {

    "return generated ontology" in new TestCase {

      val request  = Request[IO](GET, uri"/knowledge-graph/ontology")
      val response = Response[IO](httpStatuses.generateOne)
      (ontologyEndpoint
        .`GET /ontology`(_: Uri.Path)(_: Request[IO]))
        .expects(Uri.Path.empty, request)
        .returning(response.pure[IO])

      routes().call(request).status shouldBe response.status
    }
  }

  "DELETE /knowledge-graph/projects/:namespace/../:name" should {

    val projectPath = projectPaths.generateOne
    val request     = Request[IO](DELETE, Uri.unsafeFromString(s"knowledge-graph/projects/$projectPath"))

    s"return $Accepted for valid path parameters and user" in new TestCase {

      val authUser = authUsers.generateOne

      (projectPathAuthorizer.authorize _)
        .expects(projectPath, authUser.some)
        .returning(rightT[IO, EndpointSecurityException](AuthContext(authUser.some, projectPath, Set(projectPath))))

      (projectDeleteEndpoint
        .`DELETE /projects/:path`(_: model.projects.Path, _: AuthUser))
        .expects(projectPath, authUser)
        .returning(Response[IO](Accepted).pure[IO])

      routes(authUser.some).call(request).status shouldBe Accepted

      routesMetrics.clearRegistry()
    }

    s"return $Unauthorized when authentication fails" in new TestCase {
      routes(givenAuthIfNeededMiddleware(returning = OptionT.none[IO, Option[AuthUser]]))
        .call(request)
        .status shouldBe Unauthorized
    }

    s"return $NotFound when no auth header" in new TestCase {
      routes(maybeAuthUser = None).call(request).status shouldBe NotFound
    }

    s"return $NotFound when the user has no rights to the project" in new TestCase {

      val authUser = authUsers.generateOne

      (projectPathAuthorizer.authorize _)
        .expects(projectPath, authUser.some)
        .returning(leftT[IO, AuthContext[model.projects.Path]](AuthorizationFailure))

      val response = routes(authUser.some).call(request)

      response.status             shouldBe NotFound
      response.contentType        shouldBe Some(`Content-Type`(application.json))
      response.body[ErrorMessage] shouldBe InfoMessage(AuthorizationFailure.getMessage)
    }
  }

  "GET /knowledge-graph/projects/:namespace/../:name" should {

    s"return $Ok for valid path parameters" in new TestCase {

      val maybeAuthUser = authUsers.generateOption
      val projectPath   = projectPaths.generateOne

      (projectPathAuthorizer.authorize _)
        .expects(projectPath, maybeAuthUser)
        .returning(rightT[IO, EndpointSecurityException](AuthContext(maybeAuthUser, projectPath, Set(projectPath))))

      val request = Request[IO](GET, Uri.unsafeFromString(s"knowledge-graph/projects/$projectPath"))
      (projectDetailsEndpoint
        .`GET /projects/:path`(_: model.projects.Path, _: Option[AuthUser])(_: Request[IO]))
        .expects(projectPath, maybeAuthUser, request)
        .returning(Response[IO](Ok).pure[IO])

      routes(maybeAuthUser).call(request).status shouldBe Ok

      routesMetrics.clearRegistry()
    }

    s"return $NotFound for invalid project paths" in new TestCase {

      val maybeAuthUser = authUsers.generateOption
      val namespace     = nonBlankStrings().generateOne.value

      val response = routes(maybeAuthUser).call(
        Request(GET, uri"/knowledge-graph/projects" / namespace)
      )

      response.status            shouldBe NotFound
      response.contentType       shouldBe Some(`Content-Type`(application.json))
      response.body[InfoMessage] shouldBe InfoMessage("Resource not found")
    }

    s"return $Unauthorized when user authentication fails" in new TestCase {
      routes(givenAuthIfNeededMiddleware(returning = OptionT.none[IO, Option[AuthUser]]))
        .call(Request(GET, Uri.unsafeFromString(s"knowledge-graph/projects/${projectPaths.generateOne}")))
        .status shouldBe Unauthorized
    }

    s"return $NotFound when auth user has no rights for the project" in new TestCase {

      val maybeAuthUser = authUsers.generateOption
      val projectPath   = projectPaths.generateOne

      (projectPathAuthorizer.authorize _)
        .expects(projectPath, maybeAuthUser)
        .returning(leftT[IO, AuthContext[model.projects.Path]](AuthorizationFailure))

      val response = routes(maybeAuthUser).call(
        Request(GET, Uri.unsafeFromString(s"knowledge-graph/projects/$projectPath"))
      )

      response.status             shouldBe NotFound
      response.contentType        shouldBe Some(`Content-Type`(application.json))
      response.body[ErrorMessage] shouldBe InfoMessage(AuthorizationFailure.getMessage)
    }
  }

  "GET /knowledge-graph/projects/:namespace/../:name/datasets" should {

    s"return $Ok for valid path parameters" in new TestCase {
      val projectPath   = projectPaths.generateOne
      val maybeAuthUser = authUsers.generateOption

      (projectPathAuthorizer.authorize _)
        .expects(projectPath, maybeAuthUser)
        .returning(rightT[IO, EndpointSecurityException](AuthContext(maybeAuthUser, projectPath, Set(projectPath))))

      (projectDatasetsEndpoint.getProjectDatasets _).expects(projectPath).returning(IO.pure(Response[IO](Ok)))

      routes(maybeAuthUser)
        .call(Request(GET, Uri.unsafeFromString(s"/knowledge-graph/projects/$projectPath/datasets")))
        .status shouldBe Ok

      routesMetrics.clearRegistry()
    }

    s"return $Unauthorized when user authentication fails" in new TestCase {
      routes(givenAuthIfNeededMiddleware(returning = OptionT.none[IO, Option[AuthUser]]))
        .call(
          Request(GET, Uri.unsafeFromString(s"knowledge-graph/projects/${projectPaths.generateOne}/datasets"))
        )
        .status shouldBe Unauthorized
    }

    s"return $NotFound when auth user has no rights for the project" in new TestCase {
      val projectPath   = projectPaths.generateOne
      val maybeAuthUser = authUsers.generateOption

      (projectPathAuthorizer.authorize _)
        .expects(projectPath, maybeAuthUser)
        .returning(leftT[IO, AuthContext[model.projects.Path]](AuthorizationFailure))

      val response = routes(maybeAuthUser)
        .call(
          Request(GET, Uri.unsafeFromString(s"knowledge-graph/projects/$projectPath/datasets"))
        )

      response.status             shouldBe NotFound
      response.contentType        shouldBe Some(`Content-Type`(application.json))
      response.body[ErrorMessage] shouldBe InfoMessage(AuthorizationFailure.getMessage)
    }
  }

  "GET /knowledge-graph/projects/:namespace/../:name/datasets/:dsName/tags" should {
    import projects.datasets.tags.Endpoint._

    val projectPath = projectPaths.generateOne
    val datasetName = datasetNames.generateOne
    val projectDsTagsUri = projectPath.toNamespaces
      .foldLeft(uri"/knowledge-graph/projects")(_ / _.show) /
      projectPath.toName.show / "datasets" / datasetName.show / "tags"

    forAll {
      Table(
        "uri"            -> "criteria",
        projectDsTagsUri -> Criteria(projectPath, datasetName),
        pages
          .map(page =>
            projectDsTagsUri +? ("page" -> page.show) -> Criteria(projectPath,
                                                                  datasetName,
                                                                  PagingRequest.default.copy(page = page)
            )
          )
          .generateOne,
        perPages
          .map(perPage =>
            projectDsTagsUri +? ("per_page" -> perPage.show) -> Criteria(projectPath,
                                                                         datasetName,
                                                                         PagingRequest.default.copy(perPage = perPage)
            )
          )
          .generateOne
      )
    } { (uri, criteria) =>
      s"read the parameters from $uri, pass them to the endpoint and return received response" in new TestCase {
        val request = Request[IO](GET, uri)

        (projectPathAuthorizer.authorize _)
          .expects(projectPath, None)
          .returning(rightT[IO, EndpointSecurityException](AuthContext.forUnknownUser(projectPath, Set(projectPath))))

        val responseBody = jsons.generateOne
        (projectDatasetTagsEndpoint
          .`GET /projects/:path/datasets/:name/tags`(_: Criteria)(_: Request[IO]))
          .expects(criteria, request)
          .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

        val response = routes().call(request)

        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(application.json))
        response.body[Json]  shouldBe responseBody
      }
    }

    s"return $BadRequest for invalid parameter values" in new TestCase {

      val invalidPerPage = nonEmptyStrings().generateOne

      val response = routes().call(Request[IO](GET, projectDsTagsUri +? ("per_page" -> invalidPerPage)))

      response.status             shouldBe BadRequest
      response.contentType        shouldBe Some(`Content-Type`(application.json))
      response.body[ErrorMessage] shouldBe ErrorMessage(s"'$invalidPerPage' not a valid 'per_page' value")
    }

    "authenticate user from the request if given" in new TestCase {

      val maybeAuthUser = authUsers.generateOption
      val request       = Request[IO](GET, projectDsTagsUri)

      (projectPathAuthorizer.authorize _)
        .expects(projectPath, maybeAuthUser)
        .returning(rightT[IO, EndpointSecurityException](AuthContext(maybeAuthUser, projectPath, Set(projectPath))))

      val responseBody = jsons.generateOne
      (projectDatasetTagsEndpoint
        .`GET /projects/:path/datasets/:name/tags`(_: Criteria)(_: Request[IO]))
        .expects(Criteria(projectPath, datasetName, maybeUser = maybeAuthUser), request)
        .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

      routes(maybeAuthUser).call(request).status shouldBe Ok
    }

    s"return $Unauthorized when user authentication fails" in new TestCase {
      routes(givenAuthFailing()).call(Request[IO](GET, projectDsTagsUri)).status shouldBe Unauthorized
    }
  }

  "GET /knowledge-graph/projects/:projectId/files/:location/lineage" should {
    def lineageUri(projectPath: model.projects.Path, location: Location) =
      Uri.unsafeFromString(s"knowledge-graph/projects/${projectPath.show}/files/${urlEncode(location.show)}/lineage")

    s"return $Ok when the lineage is found" in new TestCase {
      val projectPath   = projectPaths.generateOne
      val location      = nodeLocations.generateOne
      val maybeAuthUser = authUsers.generateOption
      val uri           = lineageUri(projectPath, location)
      val request       = Request[IO](GET, uri)

      val responseBody = jsons.generateOne

      (projectPathAuthorizer.authorize _)
        .expects(projectPath, maybeAuthUser)
        .returning(rightT[IO, EndpointSecurityException](AuthContext(maybeAuthUser, projectPath, Set(projectPath))))

      (lineageEndpoint.`GET /lineage` _)
        .expects(projectPath, location, maybeAuthUser)
        .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

      val response = routes(maybeAuthUser).call(request)

      response.status      shouldBe Ok
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.body[Json]  shouldBe responseBody
    }

    s"return $NotFound for a lineage which isn't found" in new TestCase {
      val projectPath   = projectPaths.generateOne
      val location      = nodeLocations.generateOne
      val maybeAuthUser = authUsers.generateOption
      val uri           = lineageUri(projectPath, location)

      (projectPathAuthorizer.authorize _)
        .expects(projectPath, maybeAuthUser)
        .returning(rightT[IO, EndpointSecurityException](AuthContext(maybeAuthUser, projectPath, Set(projectPath))))

      (lineageEndpoint.`GET /lineage` _)
        .expects(projectPath, location, maybeAuthUser)
        .returning(Response[IO](NotFound).pure[IO])

      val response = routes(maybeAuthUser).call(Request[IO](GET, uri))

      response.status shouldBe NotFound
    }

    "authenticate user from the request if given" in new TestCase {

      val projectPath   = projectPaths.generateOne
      val location      = nodeLocations.generateOne
      val maybeAuthUser = authUsers.generateOption
      val request       = Request[IO](GET, lineageUri(projectPath, location))

      val responseBody = jsons.generateOne

      (projectPathAuthorizer.authorize _)
        .expects(projectPath, maybeAuthUser)
        .returning(rightT[IO, EndpointSecurityException](AuthContext(maybeAuthUser, projectPath, Set(projectPath))))

      (lineageEndpoint.`GET /lineage` _)
        .expects(projectPath, location, maybeAuthUser)
        .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

      routes(maybeAuthUser).call(request).status shouldBe Ok
    }

    s"return $Unauthorized when user authentication fails" in new TestCase {
      routes(givenAuthFailing())
        .call(Request[IO](GET, lineageUri(projectPaths.generateOne, nodeLocations.generateOne)))
        .status shouldBe Unauthorized
    }
  }

  "GET /knowledge-graph/spec.json" should {

    s"return $Ok with OpenAPI json body" in new TestCase {
      val openApiJson = jsons.generateOne
      (() => docsEndpoint.`get /spec.json`)
        .expects()
        .returning(IO.pure(Response[IO](Ok).withEntity(openApiJson)))

      val response = routes().call(Request(GET, uri"/knowledge-graph/spec.json"))

      response.status     shouldBe Ok
      response.body[Json] shouldBe openApiJson
    }
  }

  "GET /knowledge-graph/users/:id/projects" should {
    import users.projects._
    import users.projects.Endpoint._
    import users.projects.Endpoint.Criteria._

    val userId = personGitLabIds.generateOne

    forAll {
      val commonUri = uri"/knowledge-graph/users" / userId.value / "projects"
      Table(
        "uri"     -> "criteria",
        commonUri -> Criteria(userId),
        activationStates
          .map(state => commonUri +? ("state" -> state.show) -> Criteria(userId, Filters(state), PagingRequest.default))
          .generateOne,
        pages
          .map(page =>
            commonUri +? ("page" -> page.show) -> Criteria(userId, paging = PagingRequest.default.copy(page = page))
          )
          .generateOne,
        perPages
          .map(perPage =>
            commonUri +? ("per_page" -> perPage.show) -> Criteria(userId,
                                                                  paging = PagingRequest.default.copy(perPage = perPage)
            )
          )
          .generateOne
      )
    } { (uri, criteria) =>
      s"read the query parameters from $uri, pass them to the endpoint and return received response" in new TestCase {
        val request = Request[IO](GET, uri)

        val responseBody = jsons.generateOne
        (usersProjectsEndpoint.`GET /users/:id/projects` _)
          .expects(criteria, request)
          .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

        val response = routes().call(request)

        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(application.json))
        response.body[Json]  shouldBe responseBody
      }
    }

    s"return $BadRequest for invalid parameter values" in new TestCase {
      val response = routes()
        .call(
          Request[IO](GET, uri"/knowledge-graph/users" / userId.value / "projects" +? ("state" -> -1))
        )

      response.status             shouldBe BadRequest
      response.contentType        shouldBe Some(`Content-Type`(application.json))
      response.body[ErrorMessage] shouldBe ErrorMessage("'state' parameter with invalid value")
    }

    "authenticate user from the request if given" in new TestCase {

      val maybeAuthUser = authUsers.generateOption
      val request       = Request[IO](GET, uri"/knowledge-graph/users" / userId.value / "projects")

      val responseBody = jsons.generateOne
      (usersProjectsEndpoint.`GET /users/:id/projects` _)
        .expects(Criteria(userId, maybeUser = maybeAuthUser), request)
        .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

      routes(maybeAuthUser).call(request).status shouldBe Ok
    }

    s"return $Unauthorized when user authentication fails" in new TestCase {
      routes(givenAuthFailing())
        .call(Request[IO](GET, uri"/knowledge-graph/users" / userId.value / "projects"))
        .status shouldBe Unauthorized
    }
  }

  "GET /metrics" should {

    s"return $Ok with some prometheus metrics" in new TestCase {
      val response = routes().call(Request(GET, uri"/metrics"))

      response.status     shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }
  }

  "GET /ping" should {

    s"return $Ok with 'pong' body" in new TestCase {
      val response = routes().call(Request(GET, uri"/ping"))

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }
  }

  "GET /version" should {
    "return response from the version endpoint" in new TestCase {
      routes().call(Request(GET, uri"/version")).status shouldBe versionEndpointResponse.status
    }
  }

  private trait TestCase {

    val datasetsSearchEndpoint     = mock[datasets.Endpoint[IO]]
    val datasetDetailsEndpoint     = mock[datasets.details.Endpoint[IO]]
    val entitiesEndpoint           = mock[entities.Endpoint[IO]]
    val lineageEndpoint            = mock[projects.files.lineage.Endpoint[IO]]
    val ontologyEndpoint           = mock[ontology.Endpoint[IO]]
    val projectDeleteEndpoint      = mock[projects.delete.Endpoint[IO]]
    val projectDetailsEndpoint     = mock[projects.details.Endpoint[IO]]
    val projectDatasetsEndpoint    = mock[projects.datasets.Endpoint[IO]]
    val projectDatasetTagsEndpoint = mock[projects.datasets.tags.Endpoint[IO]]
    val docsEndpoint               = mock[docs.Endpoint[IO]]
    val usersProjectsEndpoint      = mock[users.projects.Endpoint[IO]]
    val projectPathAuthorizer      = mock[Authorizer[IO, model.projects.Path]]
    val datasetIdAuthorizer        = mock[Authorizer[IO, model.datasets.Identifier]]
    val routesMetrics              = TestRoutesMetrics()
    private val versionRoutes      = mock[version.Routes[IO]]

    def routes(maybeAuthUser: Option[AuthUser] = None): Resource[IO, Kleisli[IO, Request[IO], Response[IO]]] = routes(
      givenAuthIfNeededMiddleware(returning = OptionT.some[IO](maybeAuthUser))
    )

    def routes(middleware: AuthMiddleware[IO, Option[AuthUser]]): Resource[IO, Kleisli[IO, Request[IO], Response[IO]]] =
      new MicroserviceRoutes[IO](
        datasetsSearchEndpoint,
        datasetDetailsEndpoint,
        entitiesEndpoint,
        ontologyEndpoint,
        projectDeleteEndpoint,
        projectDetailsEndpoint,
        projectDatasetsEndpoint,
        projectDatasetTagsEndpoint,
        lineageEndpoint,
        docsEndpoint,
        usersProjectsEndpoint,
        middleware,
        projectPathAuthorizer,
        datasetIdAuthorizer,
        routesMetrics,
        versionRoutes
      ).routes.map(_.or(notAvailableResponse))

    val versionEndpointResponse = Response[IO](httpStatuses.generateOne)
    (versionRoutes.apply _)
      .expects()
      .returning {
        import org.http4s.dsl.io.{GET => _, _}
        HttpRoutes.of[IO] { case GET -> Root / "version" => versionEndpointResponse.pure[IO] }
      }
      .atLeastOnce()
  }
}
