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

package ch.datascience.knowledgegraph

import cats.data.{Kleisli, OptionT}
import cats.effect.{Clock, IO, Resource}
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.Path
import ch.datascience.http.ErrorMessage.ErrorMessage
import ch.datascience.http.InfoMessage._
import ch.datascience.http.rest.SortBy
import ch.datascience.http.rest.SortBy.Direction
import ch.datascience.http.rest.paging.PagingRequest
import ch.datascience.http.rest.paging.model.{Page, PerPage}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.http.server.security.model.AuthUser
import ch.datascience.http.{ErrorMessage, InfoMessage}
import ch.datascience.interpreters.TestRoutesMetrics
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.{Phrase, query}
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort._
import ch.datascience.knowledgegraph.datasets.rest._
import ch.datascience.knowledgegraph.graphql.{LineageQueryContext, QueryEndpoint, QueryRunner}
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

import scala.concurrent.ExecutionContext
import scala.language.reflectiveCalls

class MicroserviceRoutesSpec extends AnyWordSpec with MockFactory with ScalaCheckPropertyChecks with should.Matchers {

  "routes" should {

    "define a GET /ping endpoint returning OK with 'pong' body" in new TestCase {

      val response = routes(maybeAuthUser).call(
        Request(Method.GET, uri"ping")
      )

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }

    "define a GET /metrics endpoint returning OK with some prometheus metrics" in new TestCase {
      val response = routes(maybeAuthUser).call(
        Request(Method.GET, uri"metrics")
      )

      response.status     shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $Ok " +
      "when a valid 'query' and no 'sort', `page` and `per_page` parameters given" in new TestCase {

        val phrase = nonEmptyStrings().generateOne
        val request =
          Request[IO](Method.GET, uri"knowledge-graph/datasets".withQueryParam(query.parameterName, phrase))
        (datasetsSearchEndpoint
          .searchForDatasets(_: Option[Phrase], _: Sort.By, _: PagingRequest, _: Option[AuthUser]))
          .expects(Phrase(phrase).some,
                   Sort.By(TitleProperty, Direction.Asc),
                   PagingRequest(Page.first, PerPage.default),
                   maybeAuthUser
          )
          .returning(IO.pure(Response[IO](Ok)))

        val response = routes(maybeAuthUser).call(request)

        response.status shouldBe Ok
      }

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $Unauthorized when user authentication failed " +
      "when a valid 'query' and no 'sort', `page` and `per_page` parameters given" in new TestCase {
        val phrase = nonEmptyStrings().generateOne
        val request =
          Request[IO](Method.GET, uri"knowledge-graph/datasets".withQueryParam(query.parameterName, phrase))

        routes(givenAuthFailing())
          .call(request)
          .status shouldBe Unauthorized
      }

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $Ok " +
      s"when no ${query.parameterName} parameter given" in new TestCase {

        val request = Request[IO](Method.GET, uri"knowledge-graph/datasets")

        (datasetsSearchEndpoint
          .searchForDatasets(_: Option[Phrase], _: Sort.By, _: PagingRequest, _: Option[AuthUser]))
          .expects(Option.empty[Phrase],
                   Sort.By(TitleProperty, Direction.Asc),
                   PagingRequest(Page.first, PerPage.default),
                   maybeAuthUser
          )
          .returning(IO.pure(Response[IO](Ok)))

        val response = routes(maybeAuthUser).call(request)

        response.status shouldBe Ok
      }

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $Unauthorized when user authentication failed " +
      s"and when no parameters are given" in new TestCase {
        val request = Request[IO](Method.GET, uri"knowledge-graph/datasets")

        routes(givenAuthFailing())
          .call(request)
          .status shouldBe Unauthorized
      }

    Sort.properties foreach { sortProperty =>
      val sortBy = Sort.By(sortProperty, Gen.oneOf(SortBy.Direction.Asc, SortBy.Direction.Desc).generateOne)

      s"define a GET /knowledge-graph/datasets?query=<phrase>&sort=name:desc endpoint returning $Ok " +
        s"when '${query.parameterName}' and 'sort=${sortBy.property}:${sortBy.direction}' parameters given" in new TestCase {
          val phrase = phrases.generateOne
          val request = Request[IO](
            Method.GET,
            uri"knowledge-graph/datasets"
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
              uri"knowledge-graph/datasets"
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
          val request = Request[IO](
            Method.GET,
            uri"knowledge-graph/datasets"
              .withQueryParam("query", phrase.value)
              .withQueryParam("page", page.value)
              .withQueryParam("per_page", perPage.value)
          )
          (datasetsSearchEndpoint
            .searchForDatasets(_: Option[Phrase], _: Sort.By, _: PagingRequest, _: Option[AuthUser]))
            .expects(phrase.some, Sort.By(TitleProperty, Direction.Asc), PagingRequest(page, perPage), maybeAuthUser)
            .returning(IO.pure(Response[IO](Ok)))

          val response = routes(maybeAuthUser).call(request)

          response.status shouldBe Ok

          routesMetrics.clearRegistry()
        }
      }

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $Unauthorized when user authentication failed " +
      s"when query, ${PagingRequest.Decoders.page.parameterName} and ${PagingRequest.Decoders.perPage.parameterName} parameters given" in new TestCase {
        forAll(phrases, pages, perPages) { (phrase, page, perPage) =>
          val request =
            Request[IO](
              Method.GET,
              uri"knowledge-graph/datasets"
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

        val response = routes(maybeAuthUser).call {
          Request(
            Method.GET,
            uri"knowledge-graph/datasets"
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
      val id = datasetIdentifiers.generateOne

      (datasetsEndpoint.getDataset _).expects(id).returning(IO.pure(Response[IO](Ok)))

      val response = routes(maybeAuthUser).call(
        Request(Method.GET, uri"knowledge-graph/datasets" / id.value)
      )

      response.status shouldBe Ok
    }

    s"define a GET /knowledge-graph/datasets/:id endpoint returning $ServiceUnavailable when no :id path parameter given" in new TestCase {

      routes()
        .call(
          Request(Method.GET, uri"knowledge-graph/datasets/")
        )
        .status shouldBe NotFound
    }

    "define a GET /knowledge-graph/graphql endpoint" in new TestCase {

      (queryEndpoint.schema _).expects().returning(IO.pure(Response[IO](Ok)))

      val response = routes(maybeAuthUser).call(
        Request(Method.GET, uri"knowledge-graph/graphql")
      )

      response.status shouldBe Ok
    }

    "define a POST /knowledge-graph/graphql endpoint" in new TestCase {

      val request: Request[IO] = Request(Method.POST, uri"knowledge-graph/graphql")
      (queryEndpoint
        .handleQuery(_: Request[IO], _: Option[AuthUser]))
        .expects(request, maybeAuthUser)
        .returning(IO.pure(Response[IO](Ok)))

      val response = routes(maybeAuthUser).call(request)

      response.status shouldBe Ok
    }

    s"define a POST /knowledge-graph/graphql endpoint endpoint returning $Unauthorized when user authentication failed" in new TestCase {
      val request: Request[IO] = Request(Method.POST, uri"knowledge-graph/graphql")

      routes(givenAuthFailing())
        .call(request)
        .status shouldBe Unauthorized
    }

    s"define a GET /knowledge-graph/projects/:namespace/../:name endpoint returning $Ok for valid path parameters" in new TestCase {
      val projectPath = projectPaths.generateOne
      (projectEndpoint.getProject _).expects(projectPath, None).returning(IO.pure(Response[IO](Ok)))

      val response = routes(maybeAuthUser).call(
        Request(Method.GET, Uri.unsafeFromString(s"knowledge-graph/projects/$projectPath"))
      )

      response.status shouldBe Ok

      routesMetrics.clearRegistry()
    }

    s"define a GET /knowledge-graph/projects/:namespace/../:name endpoint returning $NotFound for invalid project paths" in new TestCase {
      val namespace = nonBlankStrings().generateOne.value

      val response = routes(maybeAuthUser).call(
        Request(Method.GET, uri"knowledge-graph/projects" / namespace)
      )

      response.status            shouldBe NotFound
      response.contentType       shouldBe Some(`Content-Type`(MediaType.application.json))
      response.body[InfoMessage] shouldBe InfoMessage("Resource not found")
    }

    s"define a GET /knowledge-graph/projects/:namespace/../:name/datasets endpoint returning $Ok for valid path parameters" in new TestCase {
      forAll { projectPath: Path =>
        (projectDatasetsEndpoint.getProjectDatasets _).expects(projectPath).returning(IO.pure(Response[IO](Ok)))

        val response = routes(maybeAuthUser).call(
          Request(Method.GET, Uri.unsafeFromString(s"knowledge-graph/projects/$projectPath/datasets"))
        )

        response.status shouldBe Ok

        routesMetrics.clearRegistry()
      }
    }
  }

  private implicit val clock: Clock[IO] = IO.timer(ExecutionContext.global).clock

  private trait TestCase {

    val maybeAuthUser           = authUsers.generateOption
    val queryEndpoint           = mock[IOQueryEndpoint]
    val projectEndpoint         = mock[IOProjectEndpointStub]
    val projectDatasetsEndpoint = mock[IOProjectDatasetsEndpointStub]
    val datasetsEndpoint        = mock[IODatasetEndpointStub]
    val datasetsSearchEndpoint  = mock[IODatasetsSearchEndpointStub]
    val routesMetrics           = TestRoutesMetrics()
    def routes(maybeAuthUser: Option[AuthUser] = None): Resource[IO, Kleisli[IO, Request[IO], Response[IO]]] = routes(
      givenAuthIfNeededMiddleware(returning = OptionT.some[IO](maybeAuthUser))
    )

    def routes(
        middleware: AuthMiddleware[IO, Option[AuthUser]]
    ): Resource[IO, Kleisli[IO, Request[IO], Response[IO]]] =
      new MicroserviceRoutes[IO](
        queryEndpoint,
        projectEndpoint,
        projectDatasetsEndpoint,
        datasetsEndpoint,
        datasetsSearchEndpoint,
        middleware,
        routesMetrics
      ).routes.map(_.or(notAvailableResponse))
  }

  class IOQueryEndpoint(
      queryRunner: QueryRunner[IO, LineageQueryContext[IO]]
  ) extends QueryEndpoint[IO](queryRunner)
}
