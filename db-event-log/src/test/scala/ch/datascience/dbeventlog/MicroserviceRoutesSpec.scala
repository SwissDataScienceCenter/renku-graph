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
import ch.datascience.dbeventlog.latestevents.{LatestEventsEndpoint, LatestEventsFinder}
import ch.datascience.dbeventlog.processingstatus.{ProcessingStatusEndpoint, ProcessingStatusFinder}
import ch.datascience.dbeventlog.subscriptions.{Subscriptions, SubscriptionsEndpoint}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestRoutesMetrics
import io.chrisdavenport.log4cats.Logger
import org.http4s.Method.{GET, POST}
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

    "define a POST /events endpoint" in new TestCase {
      val request        = Request[IO](POST, uri"events")
      val expectedStatus = Gen.oneOf(Created, Ok).generateOne
      (eventCreationEndpoint.addEvent _).expects(request).returning(Response[IO](expectedStatus).pure[IO])

      val response = routes.call(request)

      response.status shouldBe expectedStatus
    }

    "define a GET /events/latest endpoint" in new TestCase {
      val request = Request[IO](GET, uri"events/latest")
      (latestEventsEndpoint.findLatestEvents _).expects().returning(Response[IO](Ok).pure[IO])

      val response = routes.call(request)

      response.status shouldBe Ok
    }

    "define a GET /events/projects/:id/status endpoint" in new TestCase {
      val projectId = projectIds.generateOne

      val request = Request[IO](GET, uri"events" / "projects" / projectId.toString / "status")
      (processingStatusEndpoint.findProcessingStatus _).expects(projectId).returning(Response[IO](Ok).pure[IO])

      val response = routes.call(request)

      response.status shouldBe Ok
    }

    "define a POST /events/subscriptions?status=READY endpoint" in new TestCase {
      val request = Request[IO](POST, uri"events" / "subscriptions" withQueryParam ("status", "READY"))
      (subscriptionsEndpoint.addSubscription _).expects(request).returning(Response[IO](Accepted).pure[IO])

      val response = routes.call(request)

      response.status shouldBe Accepted
    }

    "define a POST /events/subscriptions?status=READY endpoint " +
      s"returning $BadRequest when there's no status query parameter" in new TestCase {
      val request = Request[IO](POST, uri"events" / "subscriptions")

      val response = routes.call(request)

      response.status shouldBe BadRequest
    }

    "define a POST /events/subscriptions?status=READY endpoint " +
      s"returning $BadRequest when there's value different than 'READY' for the status query parameter" in new TestCase {
      val request = Request[IO](POST, uri"events" / "subscriptions" withQueryParam ("status", "unknown"))

      val response = routes.call(request)

      response.status shouldBe BadRequest
    }

    "define a GET /metrics endpoint returning OK with some prometheus metrics" in new TestCase {
      val response = routes.call(
        Request(GET, uri"metrics")
      )

      response.status       shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }

    "define a GET /ping endpoint returning OK with 'pong' body" in new TestCase {
      val response = routes.call(
        Request(GET, uri"ping")
      )

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }
  }

  private implicit val clock: Clock[IO] = IO.timer(ExecutionContext.global).clock

  private trait TestCase {

    val latestEventsEndpoint     = mock[TestLatestEventsEndpoint]
    val eventCreationEndpoint    = mock[TestEventCreationEndpoint]
    val processingStatusEndpoint = mock[TestProcessingStatusEndpoint]
    val routesMetrics            = TestRoutesMetrics()
    val subscriptionsEndpoint    = mock[TestSubscriptionEndpoint]
    val routes = new MicroserviceRoutes[IO](
      eventCreationEndpoint,
      latestEventsEndpoint,
      processingStatusEndpoint,
      subscriptionsEndpoint,
      routesMetrics
    ).routes.map(_.or(notAvailableResponse))
  }

  class TestEventCreationEndpoint(eventAdder: EventPersister[IO], logger: Logger[IO])
      extends EventCreationEndpoint[IO](eventAdder, logger)
  class TestLatestEventsEndpoint(latestEventsFinder: LatestEventsFinder[IO], logger: Logger[IO])
      extends LatestEventsEndpoint[IO](latestEventsFinder, logger)
  class TestProcessingStatusEndpoint(processingStatusFinder: ProcessingStatusFinder[IO], logger: Logger[IO])
      extends ProcessingStatusEndpoint[IO](processingStatusFinder, logger)
  class TestSubscriptionEndpoint(subscriptions: Subscriptions[IO], logger: Logger[IO])
      extends SubscriptionsEndpoint[IO](subscriptions, logger)
}
