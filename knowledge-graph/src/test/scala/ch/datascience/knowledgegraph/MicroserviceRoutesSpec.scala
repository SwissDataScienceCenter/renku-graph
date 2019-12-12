/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import cats.effect.{Clock, IO}
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage.ErrorMessage
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.http.rest.SortBy.Direction
import ch.datascience.http.server.EndpointTester._
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.{Phrase, query}
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort._
import ch.datascience.knowledgegraph.datasets.rest._
import ch.datascience.knowledgegraph.graphql.{QueryContext, QueryEndpoint, QueryRunner}
import ch.datascience.metrics.MetricsRegistry
import org.http4s.Status._
import org.http4s._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import sangria.schema.Schema

import scala.concurrent.ExecutionContext
import scala.language.reflectiveCalls

class MicroserviceRoutesSpec extends WordSpec with MockFactory with ScalaCheckPropertyChecks {

  "routes" should {

    "define a GET /ping endpoint returning OK with 'pong' body" in new TestCase {
      val response = routes.call(
        Request(Method.GET, uri"ping")
      )

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }

    "define a GET /metrics endpoint returning OK with some prometheus metrics" in new TestCase {
      val response = routes.call(
        Request(Method.GET, uri"metrics")
      )

      response.status       shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $Ok " +
      "when a valid 'query' and no 'sort' parameter given" in new TestCase {
      val phrase = nonEmptyStrings().generateOne

      (datasetsSearchEndpoint
        .searchForDatasets(_: Phrase, _: Sort.By))
        .expects(Phrase(phrase), Sort.By(NameProperty, Direction.Asc))
        .returning(IO.pure(Response[IO](Ok)))

      val response = routes.call(
        Request(Method.GET, uri"knowledge-graph/datasets" withQueryParam (query.parameterName, phrase))
      )

      response.status shouldBe Ok
    }

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $BadRequest when invalid query parameter given" in new TestCase {

      val phrase = blankStrings().generateOne

      val response = routes.call(
        Request(Method.GET, uri"knowledge-graph/datasets" withQueryParam (query.parameterName, phrase))
      )

      response.status             shouldBe BadRequest
      response.body[ErrorMessage] shouldBe ErrorMessage(s"'${query.parameterName}' parameter with invalid value")
    }

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $ServiceUnavailable when no query parameter given" in new TestCase {
      val response = routes
        .call(Request(Method.GET, uri"knowledge-graph/datasets"))
        .status shouldBe ServiceUnavailable
    }

    s"define a GET /knowledge-graph/datasets?query=<phrase>&sort=name:desc endpoint returning $Ok when query and sort parameters given" in new TestCase {
      forAll(phrases, sortBys(Sort)) { (phrase, sortBy) =>
        (datasetsSearchEndpoint
          .searchForDatasets(_: Phrase, _: Sort.By))
          .expects(phrase, sortBy)
          .returning(IO.pure(Response[IO](Ok)))

        val response = routes.call {
          Request(
            Method.GET,
            uri"knowledge-graph/datasets"
              .withQueryParam(query.parameterName, phrase.toString)
              .withQueryParam(Sort.sort.parameterName, s"${sortBy.property}:${sortBy.direction}")
          )
        }

        response.status shouldBe Ok

        MetricsRegistry.clear()
      }
    }

    s"define a GET /knowledge-graph/datasets?query=<phrase>&sort=name:desc endpoint returning $BadRequest for invalid sort parameters" in new TestCase {
      val property = "invalid"

      val response = routes.call {
        Request(
          Method.GET,
          uri"knowledge-graph/datasets"
            .withQueryParam(query.parameterName, nonEmptyStrings().generateOne)
            .withQueryParam(Sort.sort.parameterName, s"$property:${Direction.Asc}")
        )
      }

      response.status shouldBe BadRequest
      response.body[ErrorMessage] shouldBe ErrorMessage(
        s"'$property' is not a valid sort property. Allowed properties: ${Sort.properties.mkString(", ")}"
      )
    }

    s"define a GET /knowledge-graph/datasets/:id endpoint returning $Ok when valid :id path parameter given" in new TestCase {
      val id = datasetIds.generateOne

      (datasetsEndpoint.getDataset _).expects(id).returning(IO.pure(Response[IO](Ok)))

      val response = routes.call(
        Request(Method.GET, uri"knowledge-graph/datasets" / id.value)
      )

      response.status shouldBe Ok
    }

    s"define a GET /knowledge-graph/datasets/:id endpoint returning $ServiceUnavailable when no :id path parameter given" in new TestCase {
      val id = datasetIds.generateOne

      val response = routes.call(
        Request(Method.GET, uri"knowledge-graph/datasets/")
      )

      response.status shouldBe ServiceUnavailable
    }

    "define a GET /knowledge-graph/graphql endpoint" in new TestCase {
      val id = datasetIds.generateOne

      (queryEndpoint.schema _).expects().returning(IO.pure(Response[IO](Ok)))

      val response = routes.call(
        Request(Method.GET, uri"knowledge-graph/graphql")
      )

      response.status shouldBe Ok
    }

    "define a POST /knowledge-graph/graphql endpoint" in new TestCase {
      val id = datasetIds.generateOne

      val request: Request[IO] = Request(Method.POST, uri"knowledge-graph/graphql")
      (queryEndpoint.handleQuery _).expects(request).returning(IO.pure(Response[IO](Ok)))

      val response = routes.call(request)

      response.status shouldBe Ok
    }

    s"define a GET /knowledge-graph/projects/:namespace/:name endpoint returning $Ok for valid path parameters" in new TestCase {
      val projectPath = projectPaths.generateOne

      (projectEndpoint.getProject _).expects(projectPath).returning(IO.pure(Response[IO](Ok)))

      val response = routes.call(
        Request(Method.GET, Uri.unsafeFromString(s"knowledge-graph/projects/$projectPath"))
      )

      response.status shouldBe Ok
    }

    s"define a GET /knowledge-graph/projects/:namespace/:name endpoint returning $ServiceUnavailable for missing :name" in new TestCase {
      val namespace = nonBlankStrings().generateOne.value

      val response = routes.call(
        Request(Method.GET, uri"knowledge-graph/projects" / namespace)
      )

      response.status shouldBe ServiceUnavailable
    }

    s"define a GET /knowledge-graph/projects/:namespace/:name/datasets endpoint returning $Ok for valid path parameters" in new TestCase {
      val projectPath = projectPaths.generateOne

      (projectDatasetsEndpoint.getProjectDatasets _).expects(projectPath).returning(IO.pure(Response[IO](Ok)))

      val response = routes.call(
        Request(Method.GET, Uri.unsafeFromString(s"knowledge-graph/projects/$projectPath/datasets"))
      )

      response.status shouldBe Ok
    }
  }

  private implicit val clock: Clock[IO] = IO.timer(ExecutionContext.global).clock

  private trait TestCase {
    MetricsRegistry.clear()

    val queryEndpoint           = mock[IOQueryEndpoint]
    val projectEndpoint         = mock[IOProjectEndpointStub]
    val projectDatasetsEndpoint = mock[IOProjectDatasetsEndpointStub]
    val datasetsEndpoint        = mock[IODatasetEndpointStub]
    val datasetsSearchEndpoint  = mock[IODatasetsSearchEndpointStub]
    val routes = new MicroserviceRoutes[IO](
      queryEndpoint,
      projectEndpoint,
      projectDatasetsEndpoint,
      datasetsEndpoint,
      datasetsSearchEndpoint
    ).routes.map(_.or(notAvailableResponse))
  }

  class IOQueryEndpoint(querySchema: Schema[QueryContext[IO], Unit], queryRunner: QueryRunner[IO, QueryContext[IO]])
      extends QueryEndpoint[IO](querySchema, queryRunner)
}
