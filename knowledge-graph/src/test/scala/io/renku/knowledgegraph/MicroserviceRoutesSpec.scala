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

import cats.data.EitherT.{leftT, rightT}
import cats.data.{Kleisli, OptionT}
import cats.effect.{IO, Resource}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.http.server.security.Authorizer
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model
import io.renku.graph.model.GraphModelGenerators._
import io.renku.http.ErrorMessage.ErrorMessage
import io.renku.http.InfoMessage._
import io.renku.http.rest.SortBy
import io.renku.http.rest.SortBy.Direction
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.server.EndpointTester._
import io.renku.http.server.security.EndpointSecurityException
import io.renku.http.server.security.EndpointSecurityException.AuthorizationFailure
import io.renku.http.server.security.model.AuthUser
import io.renku.http.server.version
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.interpreters.TestRoutesMetrics
import io.renku.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.{Phrase, query}
import io.renku.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import io.renku.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort._
import io.renku.knowledgegraph.datasets.rest._
import io.renku.knowledgegraph.graphql.QueryEndpoint
import io.renku.knowledgegraph.lineage.LineageGenerators._
import io.renku.knowledgegraph.lineage.model.Node.Location
import io.renku.knowledgegraph.projects.rest.ProjectEndpoint
import io.renku.testtools.IOSpec
import org.http4s.MediaType.application
import org.http4s.Method.GET
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.http4s.server.AuthMiddleware
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import model.projects.{Path => ProjectPath}

import scala.language.reflectiveCalls

class MicroserviceRoutesSpec
    extends AnyWordSpec
    with MockFactory
    with ScalaCheckPropertyChecks
    with should.Matchers
    with IOSpec {

  "GET /knowledge-graph/datasets?query=<phrase>" should {

    s"return $Ok when a valid 'query' and no 'sort', `page` and `per_page` parameters given" in new TestCase {

      val maybeAuthUser = authUsers.generateOption
      val phrase        = nonEmptyStrings().generateOne
      (datasetsSearchEndpoint
        .searchForDatasets(_: Option[Phrase], _: Sort.By, _: PagingRequest, _: Option[AuthUser]))
        .expects(Phrase(phrase).some,
                 Sort.By(TitleProperty, Direction.Asc),
                 PagingRequest(Page.first, PerPage.default),
                 maybeAuthUser
        )
        .returning(IO.pure(Response[IO](Ok)))

      routes(maybeAuthUser)
        .call(Request[IO](Method.GET, uri"/knowledge-graph/datasets".withQueryParam(query.parameterName, phrase)))
        .status shouldBe Ok
    }

    s"return $Ok when no ${query.parameterName} parameter given" in new TestCase {

      val maybeAuthUser = authUsers.generateOption

      (datasetsSearchEndpoint
        .searchForDatasets(_: Option[Phrase], _: Sort.By, _: PagingRequest, _: Option[AuthUser]))
        .expects(Option.empty[Phrase],
                 Sort.By(TitleProperty, Direction.Asc),
                 PagingRequest(Page.first, PerPage.default),
                 maybeAuthUser
        )
        .returning(IO.pure(Response[IO](Ok)))

      routes(maybeAuthUser).call(Request[IO](Method.GET, uri"/knowledge-graph/datasets")).status shouldBe Ok
    }

    s"return $Unauthorized when user authentication failed" in new TestCase {
      routes(givenAuthFailing())
        .call(Request[IO](Method.GET, uri"/knowledge-graph/datasets"))
        .status shouldBe Unauthorized
    }

    Sort.properties foreach { sortProperty =>
      val sortBy = Sort.By(sortProperty, Gen.oneOf(SortBy.Direction.Asc, SortBy.Direction.Desc).generateOne)

      s"return $Ok when '${query.parameterName}' and 'sort=${sortBy.property}:${sortBy.direction}' parameters given" in new TestCase {
        val maybeAuthUser = authUsers.generateOption

        val phrase = phrases.generateOne
        val request = Request[IO](
          Method.GET,
          uri"/knowledge-graph/datasets"
            .withQueryParam("query", phrase.value)
            .withQueryParam("sort", s"${sortBy.property}:${sortBy.direction}")
        )
        (datasetsSearchEndpoint
          .searchForDatasets(_: Option[Phrase], _: Sort.By, _: PagingRequest, _: Option[AuthUser]))
          .expects(phrase.some, sortBy, PagingRequest.default, maybeAuthUser)
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
          Method.GET,
          uri"/knowledge-graph/datasets"
            .withQueryParam("query", phrase.value)
            .withQueryParam("page", page.value)
            .withQueryParam("per_page", perPage.value)
        )
        (datasetsSearchEndpoint
          .searchForDatasets(_: Option[Phrase], _: Sort.By, _: PagingRequest, _: Option[AuthUser]))
          .expects(phrase.some, Sort.By(TitleProperty, Direction.Asc), PagingRequest(page, perPage), maybeAuthUser)
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
            Method.GET,
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

      (datasetEndpoint.getDataset _).expects(id, authContext).returning(IO.pure(Response[IO](Ok)))

      routes(maybeAuthUser)
        .call(Request(Method.GET, uri"/knowledge-graph/datasets" / id.value))
        .status shouldBe Ok
    }

    s"return $NotFound when no :id path parameter given" in new TestCase {
      routes()
        .call(Request(Method.GET, uri"/knowledge-graph/datasets/"))
        .status shouldBe NotFound
    }

    s"return $Unauthorized when user authentication failed" in new TestCase {
      routes(givenAuthIfNeededMiddleware(returning = OptionT.none[IO, Option[AuthUser]]))
        .call(Request(Method.GET, uri"/knowledge-graph/datasets" / datasetIdentifiers.generateOne.value))
        .status shouldBe Unauthorized
    }

    s"return $NotFound when user has no rights for the project the dataset belongs to" in new TestCase {
      val id            = datasetIdentifiers.generateOne
      val maybeAuthUser = authUsers.generateOption

      (datasetIdAuthorizer.authorize _)
        .expects(id, maybeAuthUser)
        .returning(leftT[IO, AuthContext[model.datasets.Identifier]](AuthorizationFailure))

      val response = routes(maybeAuthUser).call(Request(Method.GET, uri"/knowledge-graph/datasets" / id.show))

      response.status             shouldBe NotFound
      response.contentType        shouldBe Some(`Content-Type`(application.json))
      response.body[ErrorMessage] shouldBe InfoMessage(AuthorizationFailure.getMessage)
    }
  }
  "GET /knowledge-graph/projects/:projectId/files/:location/lineage" should {
    def lineageUri(projectPath: ProjectPath, location: Location) =
      Uri.unsafeFromString(s"knowledge-graph/projects/${projectPath.value}/files/${location.value}/lineage")

    s"return $Ok when the lineage is found" in new TestCase {
      val projectPath   = projectPaths.generateOne
      val location      = nodeLocations.generateOne
      val maybeAuthUser = authUsers.generateOption
      val uri           = lineageUri(projectPath, location)
      val request       = Request[IO](Method.GET, uri)

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

      val response = routes(maybeAuthUser).call(Request[IO](Method.GET, uri))

      response.status shouldBe NotFound
    }

    "authenticate user from the request if given" in new TestCase {

      val projectPath   = projectPaths.generateOne
      val location      = nodeLocations.generateOne
      val maybeAuthUser = authUsers.generateOption
      val request       = Request[IO](Method.GET, lineageUri(projectPath, location))

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
        .call(Request[IO](Method.GET, lineageUri(projectPaths.generateOne, nodeLocations.generateOne)))
        .status shouldBe Unauthorized
    }

  }

  "GET /knowledge-graph/entities" should {
    import entities.Endpoint.Criteria.Sorting._
    import entities.Endpoint.Criteria._
    import entities.Endpoint._
    import entities._

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
          .toGeneratorOfList(minElements = 2)
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
          .toGeneratorOfList(minElements = 2)
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
          .toGeneratorOfList(minElements = 2)
          .map { list =>
            val uri = uri"/knowledge-graph/entities" ++? ("visibility" -> list.map(_.show))
            uri -> Criteria(Filters(visibilities = list.toSet))
          }
          .generateOne,
        dateParams
          .map(d =>
            uri"/knowledge-graph/entities" +? ("date" -> d.value.toString) -> Criteria(Filters(maybeDate = d.some))
          )
          .generateOne,
        sortingDirections
          .map(dir =>
            uri"/knowledge-graph/entities" +? ("sort" -> s"matchingScore:$dir") -> Criteria(sorting =
              Sorting.By(ByMatchingScore, dir)
            )
          )
          .generateOne,
        sortingDirections
          .map(dir =>
            uri"/knowledge-graph/entities" +? ("sort" -> s"name:$dir") -> Criteria(sorting = Sorting.By(ByName, dir))
          )
          .generateOne,
        sortingDirections
          .map(dir =>
            uri"/knowledge-graph/entities" +? ("sort" -> s"date:$dir") -> Criteria(sorting = Sorting.By(ByDate, dir))
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
        val request = Request[IO](Method.GET, uri)

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
          Request[IO](Method.GET, uri"/knowledge-graph/entities" +? ("visibility" -> nonEmptyStrings().generateOne))
        )

      response.status             shouldBe BadRequest
      response.contentType        shouldBe Some(`Content-Type`(application.json))
      response.body[ErrorMessage] shouldBe ErrorMessage("'visibility' parameter with invalid value")
    }

    "authenticate user from the request if given" in new TestCase {
      val maybeAuthUser = authUsers.generateOption
      val request       = Request[IO](Method.GET, uri"/knowledge-graph/entities")

      val responseBody = jsons.generateOne
      (entitiesEndpoint.`GET /entities` _)
        .expects(Criteria(maybeUser = maybeAuthUser), request)
        .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

      routes(maybeAuthUser).call(request).status shouldBe Ok
    }

    s"return $Unauthorized when user authentication fails" in new TestCase {
      routes(givenAuthFailing())
        .call(Request[IO](Method.GET, uri"/knowledge-graph/entities"))
        .status shouldBe Unauthorized
    }
  }

  "GET /knowledge-graph/graphql" should {

    "pass the response from the QueryEndpoint.schema" in new TestCase {

      (queryEndpoint.schema _).expects().returning(Response[IO](Ok).pure[IO])

      routes()
        .call(Request(Method.GET, uri"/knowledge-graph/graphql"))
        .status shouldBe Ok
    }
  }

  "POST /knowledge-graph/graphql" should {

    "pass the response from the QueryEndpoint.handleQuery" in new TestCase {
      val maybeAuthUser = authUsers.generateOption

      val request: Request[IO] = Request(Method.POST, uri"/knowledge-graph/graphql")
      (queryEndpoint
        .handleQuery(_: Request[IO], _: Option[AuthUser]))
        .expects(request, maybeAuthUser)
        .returning(IO.pure(Response[IO](Ok)))

      routes(maybeAuthUser).call(request).status shouldBe Ok
    }

    s"return $Unauthorized when user authentication failed" in new TestCase {
      routes(givenAuthFailing())
        .call(Request(Method.POST, uri"/knowledge-graph/graphql"))
        .status shouldBe Unauthorized
    }
  }

  "GET /knowledge-graph/projects/:namespace/../:name" should {

    s"return $Ok for valid path parameters" in new TestCase {
      val maybeAuthUser = authUsers.generateOption
      val projectPath   = projectPaths.generateOne

      (projectPathAuthorizer.authorize _)
        .expects(projectPath, maybeAuthUser)
        .returning(rightT[IO, EndpointSecurityException](AuthContext(maybeAuthUser, projectPath, Set(projectPath))))

      (projectEndpoint.getProject _).expects(projectPath, maybeAuthUser).returning(Response[IO](Ok).pure[IO])

      routes(maybeAuthUser)
        .call(Request(Method.GET, Uri.unsafeFromString(s"knowledge-graph/projects/$projectPath")))
        .status shouldBe Ok

      routesMetrics.clearRegistry()
    }

    s"return $NotFound for invalid project paths" in new TestCase {
      val maybeAuthUser = authUsers.generateOption
      val namespace     = nonBlankStrings().generateOne.value

      val response = routes(maybeAuthUser).call(
        Request(Method.GET, uri"/knowledge-graph/projects" / namespace)
      )

      response.status            shouldBe NotFound
      response.contentType       shouldBe Some(`Content-Type`(application.json))
      response.body[InfoMessage] shouldBe InfoMessage("Resource not found")
    }

    s"return $Unauthorized when user authentication fails" in new TestCase {
      routes(givenAuthIfNeededMiddleware(returning = OptionT.none[IO, Option[AuthUser]]))
        .call(Request(Method.GET, Uri.unsafeFromString(s"knowledge-graph/projects/${projectPaths.generateOne}")))
        .status shouldBe Unauthorized
    }

    s"return $NotFound when auth user has no rights for the project" in new TestCase {
      val maybeAuthUser = authUsers.generateOption
      val projectPath   = projectPaths.generateOne

      (projectPathAuthorizer.authorize _)
        .expects(projectPath, maybeAuthUser)
        .returning(leftT[IO, AuthContext[model.projects.Path]](AuthorizationFailure))

      val response = routes(maybeAuthUser).call(
        Request(Method.GET, Uri.unsafeFromString(s"knowledge-graph/projects/$projectPath"))
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
        .call(Request(Method.GET, Uri.unsafeFromString(s"/knowledge-graph/projects/$projectPath/datasets")))
        .status shouldBe Ok

      routesMetrics.clearRegistry()
    }

    s"return $Unauthorized when user authentication fails" in new TestCase {
      routes(givenAuthIfNeededMiddleware(returning = OptionT.none[IO, Option[AuthUser]]))
        .call(
          Request(Method.GET, Uri.unsafeFromString(s"knowledge-graph/projects/${projectPaths.generateOne}/datasets"))
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
          Request(Method.GET, Uri.unsafeFromString(s"knowledge-graph/projects/$projectPath/datasets"))
        )

      response.status             shouldBe NotFound
      response.contentType        shouldBe Some(`Content-Type`(application.json))
      response.body[ErrorMessage] shouldBe InfoMessage(AuthorizationFailure.getMessage)
    }
  }

  "GET /metrics" should {

    s"return $Ok with some prometheus metrics" in new TestCase {
      val response = routes().call(Request(Method.GET, uri"/metrics"))

      response.status     shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }
  }

  "GET /ping" should {

    s"return $Ok with 'pong' body" in new TestCase {
      val response = routes().call(Request(Method.GET, uri"/ping"))

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

    val datasetsSearchEndpoint  = mock[DatasetsSearchEndpoint[IO]]
    val datasetEndpoint         = mock[DatasetEndpoint[IO]]
    val entitiesEndpoint        = mock[entities.Endpoint[IO]]
    val queryEndpoint           = mock[QueryEndpoint[IO]]
    val lineageEndpoint         = mock[lineage.Endpoint[IO]]
    val projectEndpoint         = mock[ProjectEndpoint[IO]]
    val projectDatasetsEndpoint = mock[ProjectDatasetsEndpoint[IO]]
    val projectPathAuthorizer   = mock[Authorizer[IO, model.projects.Path]]
    val datasetIdAuthorizer     = mock[Authorizer[IO, model.datasets.Identifier]]
    val routesMetrics           = TestRoutesMetrics()
    val versionRoutes           = mock[version.Routes[IO]]

    def routes(maybeAuthUser: Option[AuthUser] = None): Resource[IO, Kleisli[IO, Request[IO], Response[IO]]] = routes(
      givenAuthIfNeededMiddleware(returning = OptionT.some[IO](maybeAuthUser))
    )

    def routes(middleware: AuthMiddleware[IO, Option[AuthUser]]): Resource[IO, Kleisli[IO, Request[IO], Response[IO]]] =
      new MicroserviceRoutes[IO](
        datasetsSearchEndpoint,
        datasetEndpoint,
        entitiesEndpoint,
        queryEndpoint,
        lineageEndpoint,
        projectEndpoint,
        projectDatasetsEndpoint,
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
