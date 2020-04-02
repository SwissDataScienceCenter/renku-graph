/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.dbeventlog

import cats.effect.{Clock, IO}
import cats.implicits._
import ch.datascience.dbeventlog.creation.{EventCreationEndpoint, EventPersister}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestRoutesMetrics
import io.chrisdavenport.log4cats.Logger
import org.http4s.Status._
import org.http4s._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext
import scala.language.reflectiveCalls

class MicroserviceRoutesSpec extends WordSpec with MockFactory {

  "routes" should {

    "define a POST /knowledge-graph/graphql endpoint" in new TestCase {
      val request        = Request[IO](Method.POST, uri"events")
      val expectedStatus = Gen.oneOf(Created, Ok).generateOne
      (eventCreationEndpoint.addEvent _).expects(request).returning(Response[IO](expectedStatus).pure[IO])

      val response = routes.call(request)

      response.status shouldBe expectedStatus
    }

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
  }

  private implicit val clock: Clock[IO] = IO.timer(ExecutionContext.global).clock

  private trait TestCase {

    val eventCreationEndpoint = mock[TestEventCreationEndpoint]
    val routesMetrics         = TestRoutesMetrics()
    val routes = new MicroserviceRoutes[IO](
      eventCreationEndpoint,
      routesMetrics
    ).routes.map(_.or(notAvailableResponse))
  }

  class TestEventCreationEndpoint(eventAdder: EventPersister[IO], logger: Logger[IO])
      extends EventCreationEndpoint[IO](eventAdder, logger)
}
