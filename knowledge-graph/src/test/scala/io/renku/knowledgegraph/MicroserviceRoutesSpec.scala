/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.http.server.security.Authorizer
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.datasets
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
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.interpreters.TestRoutesMetrics
import io.renku.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.{Phrase, query}
import io.renku.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import io.renku.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort._
import io.renku.knowledgegraph.datasets.rest._
import io.renku.knowledgegraph.graphql.QueryEndpoint
import io.renku.knowledgegraph.projects.rest.ProjectEndpoint
import io.renku.testtools.IOSpec
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

import scala.language.reflectiveCalls

class MicroserviceRoutesSpec
    extends AnyWordSpec
    with MockFactory
    with ScalaCheckPropertyChecks
    with should.Matchers
    with IOSpec {

  "routes" should {

    "define a GET /ping endpoint returning OK with 'pong' body" in new TestCase {
      val response = routes().call(Request(Method.GET, uri"/ping"))

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }

    "define a GET /metrics endpoint returning OK with some prometheus metrics" in new TestCase {
      val response = routes().call(Request(Method.GET, uri"/metrics"))

      response.status     shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $Ok " +
      "when a valid 'query' and no 'sort', `page` and `per_page` parameters given" in new TestCase {

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

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $Unauthorized when user authentication failed " +
      "when a valid 'query' and no 'sort', `page` and `per_page` parameters given" in new TestCase {
        val phrase = nonEmptyStrings().generateOne

        routes(givenAuthFailing())
          .call(Request[IO](Method.GET, uri"/knowledge-graph/datasets".withQueryParam(query.parameterName, phrase)))
          .status shouldBe Unauthorized
      }

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $Ok " +
      s"when no ${query.parameterName} parameter given" in new TestCase {

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

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $Unauthorized when user authentication failed " +
      s"and when no parameters are given" in new TestCase {
        val request = Request[IO](Method.GET, uri"/knowledge-graph/datasets")

        routes(givenAuthFailing())
          .call(request)
          .status shouldBe Unauthorized
      }

    Sort.properties foreach { sortProperty =>
      val sortBy = Sort.By(sortProperty, Gen.oneOf(SortBy.Direction.Asc, SortBy.Direction.Desc).generateOne)

      s"define a GET /knowledge-graph/datasets?query=<phrase>&sort=name:desc endpoint returning $Ok " +
        s"when '${query.parameterName}' and 'sort=${sortBy.property}:${sortBy.direction}' parameters given" in new TestCase {
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

      s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $Unauthorized when user authentication failed " +
        s"when '${query.parameterName}' and 'sort=${sortBy.property}:${sortBy.direction}' parameters given" in new TestCase {
          val phrase = phrases.generateOne
          val request =
            Request[IO](
              Method.GET,
              uri"/knowledge-graph/datasets"
                .withQueryParam("query", phrase.value)
                .withQueryParam("sort", s"${sortBy.property}:${sortBy.direction}")
            )

          routes(givenAuthFailing())
            .call(request)
            .status shouldBe Unauthorized
        }
    }

    s"define a GET /knowledge-graph/datasets?query=<phrase>&page=<page>&per_page=<per_page> endpoint returning $Ok " +
      s"when query, ${PagingRequest.Decoders.page.parameterName} and ${PagingRequest.Decoders.perPage.parameterName} parameters given" in new TestCase {
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

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $Unauthorized when user authentication failed " +
      s"when query, ${PagingRequest.Decoders.page.parameterName} and ${PagingRequest.Decoders.perPage.parameterName} parameters given" in new TestCase {
        forAll(phrases, pages, perPages) { (phrase, page, perPage) =>
          val request =
            Request[IO](
              Method.GET,
              uri"/knowledge-graph/datasets"
                .withQueryParam("query", phrase.value)
                .withQueryParam("page", page.value)
                .withQueryParam("per_page", perPage.value)
            )

          routes(givenAuthFailing())
            .call(request)
            .status shouldBe Unauthorized
        }
      }

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $BadRequest " +
      s"for invalid ${query.parameterName} parameter " +
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

    s"define a GET /knowledge-graph/datasets/:id endpoint returning $Ok when valid :id path parameter given" in new TestCase {
      val id            = datasetIdentifiers.generateOne
      val maybeAuthUser = authUsers.generateOption

      val authContext: AuthContext[datasets.Identifier] = AuthContext(maybeAuthUser, id, projectPaths.generateSet())
      (datasetIdAuthorizer.authorize _)
        .expects(id, maybeAuthUser)
        .returning(rightT[IO, EndpointSecurityException](authContext))

      (datasetsEndpoint.getDataset _).expects(id, authContext).returning(IO.pure(Response[IO](Ok)))

      routes(maybeAuthUser)
        .call(Request(Method.GET, uri"/knowledge-graph/datasets" / id.value))
        .status shouldBe Ok
    }

    s"define a GET /knowledge-graph/datasets/:id endpoint returning $NotFound when no :id path parameter given" in new TestCase {
      routes()
        .call(Request(Method.GET, uri"/knowledge-graph/datasets/"))
        .status shouldBe NotFound
    }

    s"define a GET /knowledge-graph/datasets/:id endpoint returning $Unauthorized when user is not authorized" in new TestCase {
      routes(givenAuthIfNeededMiddleware(returning = OptionT.none[IO, Option[AuthUser]]))
        .call(Request(Method.GET, uri"/knowledge-graph/datasets" / datasetIdentifiers.generateOne.value))
        .status shouldBe Unauthorized
    }

    s"define a GET /knowledge-graph/datasets/:id endpoint returning $NotFound when user has no rights for the project the dataset belongs to" in new TestCase {
      val id            = datasetIdentifiers.generateOne
      val maybeAuthUser = authUsers.generateOption

      (datasetIdAuthorizer.authorize _)
        .expects(id, maybeAuthUser)
        .returning(leftT[IO, AuthContext[model.datasets.Identifier]](AuthorizationFailure))

      val response = routes(maybeAuthUser).call(Request(Method.GET, uri"/knowledge-graph/datasets" / id.show))

      response.status             shouldBe NotFound
      response.contentType        shouldBe Some(`Content-Type`(MediaType.application.json))
      response.body[ErrorMessage] shouldBe InfoMessage(AuthorizationFailure.getMessage)
    }

    "define a GET /knowledge-graph/graphql endpoint" in new TestCase {

      (queryEndpoint.schema _).expects().returning(IO.pure(Response[IO](Ok)))

      routes()
        .call(Request(Method.GET, uri"/knowledge-graph/graphql"))
        .status shouldBe Ok
    }

    "define a POST /knowledge-graph/graphql endpoint" in new TestCase {
      val maybeAuthUser = authUsers.generateOption

      val request: Request[IO] = Request(Method.POST, uri"/knowledge-graph/graphql")
      (queryEndpoint
        .handleQuery(_: Request[IO], _: Option[AuthUser]))
        .expects(request, maybeAuthUser)
        .returning(IO.pure(Response[IO](Ok)))

      routes(maybeAuthUser).call(request).status shouldBe Ok
    }

    s"define a POST /knowledge-graph/graphql endpoint endpoint returning $Unauthorized when user authentication failed" in new TestCase {
      val request: Request[IO] = Request(Method.POST, uri"/knowledge-graph/graphql")

      routes(givenAuthFailing())
        .call(request)
        .status shouldBe Unauthorized
    }

    s"define a GET /knowledge-graph/projects/:namespace/../:name endpoint returning $Ok for valid path parameters" in new TestCase {
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

    s"define a GET /knowledge-graph/projects/:namespace/../:name endpoint returning $NotFound for invalid project paths" in new TestCase {
      val maybeAuthUser = authUsers.generateOption
      val namespace     = nonBlankStrings().generateOne.value

      val response = routes(maybeAuthUser).call(
        Request(Method.GET, uri"/knowledge-graph/projects" / namespace)
      )

      response.status            shouldBe NotFound
      response.contentType       shouldBe Some(`Content-Type`(MediaType.application.json))
      response.body[InfoMessage] shouldBe InfoMessage("Resource not found")
    }

    s"define a GET /knowledge-graph/projects/:namespace/../:name endpoint returning $Unauthorized when user is not authorized" in new TestCase {
      routes(givenAuthIfNeededMiddleware(returning = OptionT.none[IO, Option[AuthUser]]))
        .call(Request(Method.GET, Uri.unsafeFromString(s"knowledge-graph/projects/${projectPaths.generateOne}")))
        .status shouldBe Unauthorized
    }

    s"define a GET /knowledge-graph/projects/:namespace/../:name endpoint returning $NotFound when user has no rights for the project" in new TestCase {
      val maybeAuthUser = authUsers.generateOption
      val projectPath   = projectPaths.generateOne

      (projectPathAuthorizer.authorize _)
        .expects(projectPath, maybeAuthUser)
        .returning(leftT[IO, AuthContext[model.projects.Path]](AuthorizationFailure))

      val response = routes(maybeAuthUser).call(
        Request(Method.GET, Uri.unsafeFromString(s"knowledge-graph/projects/$projectPath"))
      )

      response.status             shouldBe NotFound
      response.contentType        shouldBe Some(`Content-Type`(MediaType.application.json))
      response.body[ErrorMessage] shouldBe InfoMessage(AuthorizationFailure.getMessage)
    }

    s"define a GET /knowledge-graph/projects/:namespace/../:name/datasets endpoint returning $Ok for valid path parameters" in new TestCase {
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

    s"define a GET /knowledge-graph/projects/:namespace/../:name/datasets endpoint returning $Unauthorized when user is not authorized" in new TestCase {
      routes(givenAuthIfNeededMiddleware(returning = OptionT.none[IO, Option[AuthUser]]))
        .call(
          Request(Method.GET, Uri.unsafeFromString(s"knowledge-graph/projects/${projectPaths.generateOne}/datasets"))
        )
        .status shouldBe Unauthorized
    }

    s"define a GET /knowledge-graph/projects/:namespace/../:name/datasets endpoint returning $NotFound when user has no rights for the project" in new TestCase {
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
      response.contentType        shouldBe Some(`Content-Type`(MediaType.application.json))
      response.body[ErrorMessage] shouldBe InfoMessage(AuthorizationFailure.getMessage)
    }
  }

  private trait TestCase {

    val queryEndpoint           = mock[QueryEndpoint[IO]]
    val projectEndpoint         = mock[ProjectEndpoint[IO]]
    val projectDatasetsEndpoint = mock[ProjectDatasetsEndpoint[IO]]
    val datasetsEndpoint        = mock[DatasetEndpoint[IO]]
    val datasetsSearchEndpoint  = mock[DatasetsSearchEndpoint[IO]]
    val projectPathAuthorizer   = mock[Authorizer[IO, model.projects.Path]]
    val datasetIdAuthorizer     = mock[Authorizer[IO, model.datasets.Identifier]]
    val routesMetrics           = TestRoutesMetrics()

    def routes(maybeAuthUser: Option[AuthUser] = None): Resource[IO, Kleisli[IO, Request[IO], Response[IO]]] = routes(
      givenAuthIfNeededMiddleware(returning = OptionT.some[IO](maybeAuthUser))
    )

    def routes(middleware: AuthMiddleware[IO, Option[AuthUser]]): Resource[IO, Kleisli[IO, Request[IO], Response[IO]]] =
      new MicroserviceRoutes[IO](
        queryEndpoint,
        projectEndpoint,
        projectDatasetsEndpoint,
        datasetsEndpoint,
        datasetsSearchEndpoint,
        middleware,
        projectPathAuthorizer,
        datasetIdAuthorizer,
        routesMetrics
      ).routes.map(_.or(notAvailableResponse))
  }
}
