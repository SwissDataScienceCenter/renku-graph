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

import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
import ch.datascience.controllers.ErrorMessage.ErrorMessage
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.server.EndpointTester._
import ch.datascience.http.server.QueryParameterTools._
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Phrase
import ch.datascience.knowledgegraph.datasets.rest._
import ch.datascience.knowledgegraph.graphql.{QueryContext, QueryEndpoint, QueryRunner}
import org.http4s.Status._
import org.http4s._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import sangria.schema.Schema

import scala.language.reflectiveCalls

class MicroserviceRoutesSpec extends WordSpec with MockFactory {

  "routes" should {

    "define a GET /ping endpoint returning OK with 'pong' body" in new TestCase {
      val response = routes.call(
        Request(Method.GET, uri"ping")
      )

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $Ok when valid query parameter given" in new TestCase {
      val phrase = nonEmptyStrings().generateOne

      (datasetsSearchEndpoint.searchForDatasets _).expects(Phrase(phrase)).returning(IO.pure(Response[IO](Ok)))

      val response = routes.call(
        Request(Method.GET, uri"knowledge-graph/datasets" withQueryParam ("query", phrase))
      )

      response.status shouldBe Ok
    }

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $BadRequest when invalid query parameter given" in new TestCase {

      val phrase = blankStrings().generateOne

      val response = routes.call(
        Request(Method.GET, uri"knowledge-graph/datasets" withQueryParam ("query", phrase))
      )

      val expectedResponse = toBadRequest[IO](DatasetsSearchEndpoint.QueryParameter.name)(
        Phrase
          .from(phrase)
          .leftMap(_.getMessage)
          .leftMap(ParseFailure(_, ""))
          .leftMap(failure => NonEmptyList.of(failure))
          .swap
          .getOrElse(fail("Phrase instantiation error expected"))
      ).unsafeRunSync()

      response.status             shouldBe expectedResponse.status
      response.body[ErrorMessage] shouldBe expectedResponse.as[ErrorMessage].unsafeRunSync()
    }

    s"define a GET /knowledge-graph/datasets?query=<phrase> endpoint returning $ServiceUnavailable when no query parameter given" in new TestCase {
      val response = routes
        .call(Request(Method.GET, uri"knowledge-graph/datasets"))
        .status shouldBe ServiceUnavailable
    }
  }

  private trait TestCase {
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
    ).routes.or(notAvailableResponse)
  }

  class IOQueryEndpoint(querySchema: Schema[QueryContext[IO], Unit], queryRunner: QueryRunner[IO, QueryContext[IO]])
      extends QueryEndpoint[IO](querySchema, queryRunner)
}
