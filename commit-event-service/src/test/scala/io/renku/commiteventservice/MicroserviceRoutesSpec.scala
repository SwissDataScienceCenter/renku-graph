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

package io.renku.commiteventservice

import cats.effect.IO
import cats.syntax.all._
import io.renku.commiteventservice.events.EventEndpoint
import io.renku.generators.Generators.Implicits._
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestRoutesMetrics
import io.renku.testtools.IOSpec
import org.http4s.Method.{GET, POST}
import org.http4s.Status._
import org.http4s._
import org.http4s.implicits._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.language.reflectiveCalls

class MicroserviceRoutesSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "routes" should {

    "define a POST /events endpoint" in new TestCase {
      val request        = Request[IO](POST, uri"/events")
      val expectedStatus = Gen.oneOf(Accepted, BadRequest, InternalServerError, TooManyRequests).generateOne
      (eventEndpoint.processEvent _).expects(request).returning(Response[IO](expectedStatus).pure[IO])

      val response = routes.call(request)

      response.status shouldBe expectedStatus
    }

    "define a GET /metrics endpoint returning OK with some prometheus metrics" in new TestCase {
      val response = routes.call(
        Request(GET, uri"/metrics")
      )

      response.status     shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }

    "define a GET /ping endpoint returning OK with 'pong' body" in new TestCase {
      val response = routes.call(Request(GET, uri"/ping"))

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }
  }

  private trait TestCase {
    val eventEndpoint = mock[EventEndpoint[IO]]
    val routesMetrics = TestRoutesMetrics()
    val routes = new MicroserviceRoutes[IO](
      eventEndpoint,
      routesMetrics
    ).routes.map(_.or(notAvailableResponse))
  }
}
