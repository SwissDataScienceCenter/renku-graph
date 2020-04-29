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

package io.renku.eventlog

import cats.effect.{Clock, IO}
import cats.implicits._
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage.ErrorMessage
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.compoundEventIds
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.graph.model.projects
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestRoutesMetrics
import ch.datascience.metrics.LabeledGauge
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.creation.{EventCreationEndpoint, EventPersister}
import io.renku.eventlog.eventspatching.EventsPatchingEndpoint
import io.renku.eventlog.latestevents.{LatestEventsEndpoint, LatestEventsFinder}
import io.renku.eventlog.processingstatus.{ProcessingStatusEndpoint, ProcessingStatusFinder}
import io.renku.eventlog.statuschange.{StatusChangeEndpoint, StatusUpdatesRunner}
import io.renku.eventlog.subscriptions.{Subscriptions, SubscriptionsEndpoint}
import org.http4s.MediaType.application
import org.http4s.Method.{GET, PATCH, POST}
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
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

    "define a PATCH /events endpoint" in new TestCase {

      val request = Request[IO](PATCH, uri"events")

      (eventsPatchingEndpoint.triggerEventsPatching _).expects(request).returning(Response[IO](Accepted).pure[IO])

      val response = routes.call(request)

      response.status shouldBe Accepted
    }

    "define a GET /events/latest endpoint" in new TestCase {
      val request = Request[IO](GET, uri"events/latest")
      (latestEventsEndpoint.findLatestEvents _).expects().returning(Response[IO](Ok).pure[IO])

      val response = routes.call(request)

      response.status shouldBe Ok
    }

    "define a PATCH /events/:id/projects/:id/status endpoint" in new TestCase {
      val eventId = compoundEventIds.generateOne

      val request = Request[IO](
        method = PATCH,
        uri"events" / eventId.id.toString / "projects" / eventId.projectId.toString / "status"
      )

      (statusChangeEndpoint.changeStatus _).expects(eventId, request).returning(Response[IO](Ok).pure[IO])

      val response = routes.call(request)

      response.status shouldBe Ok
    }

    "define a GET /metrics endpoint returning OK with some prometheus metrics" in new TestCase {
      val response = routes.call(
        Request(GET, uri"metrics")
      )

      response.status       shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }

    "define a GET /ping endpoint returning OK with 'pong' body" in new TestCase {
      val response = routes.call(Request(GET, uri"ping"))

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }

    "define a GET /processing-status?project-id=:id endpoint" in new TestCase {
      val projectId = projectIds.generateOne

      val request = Request[IO](GET, uri"processing-status" withQueryParam ("project-id", projectId.toString))
      (processingStatusEndpoint.findProcessingStatus _).expects(projectId).returning(Response[IO](Ok).pure[IO])

      val response = routes.call(request)

      response.status shouldBe Ok
    }

    "define a GET /processing-status?project-id=:id endpoint " +
      s"returning $BadRequest if no project-id parameter is given" in new TestCase {
      val projectId = projectIds.generateOne

      val request = Request[IO](GET, uri"processing-status")

      val response = routes.call(request)

      response.status             shouldBe BadRequest
      response.contentType        shouldBe Some(`Content-Type`(application.json))
      response.body[ErrorMessage] shouldBe ErrorMessage("No 'project-id' parameter")
    }

    "define a GET /processing-status?project-id=:id endpoint " +
      s"returning $BadRequest if illegal project-id parameter value is given" in new TestCase {
      val projectId = projectIds.generateOne

      val request = Request[IO](GET, uri"processing-status" withQueryParam ("project-id", "non int value"))

      val response = routes.call(request)

      response.status             shouldBe BadRequest
      response.contentType        shouldBe Some(`Content-Type`(application.json))
      response.body[ErrorMessage] shouldBe ErrorMessage("'project-id' parameter with invalid value")
    }

    "define a POST /subscriptions endpoint" in new TestCase {
      val request = Request[IO](POST, uri"subscriptions")

      (subscriptionsEndpoint.addSubscription _).expects(request).returning(Response[IO](Accepted).pure[IO])

      val response = routes.call(request)

      response.status shouldBe Accepted
    }
  }

  private implicit val clock: Clock[IO] = IO.timer(ExecutionContext.global).clock

  private trait TestCase {

    val latestEventsEndpoint     = mock[TestLatestEventsEndpoint]
    val eventCreationEndpoint    = mock[TestEventCreationEndpoint]
    val processingStatusEndpoint = mock[TestProcessingStatusEndpoint]
    val eventsPatchingEndpoint   = mock[EventsPatchingEndpoint[IO]]
    val routesMetrics            = TestRoutesMetrics()
    val statusChangeEndpoint     = mock[TestStatusChangeEndpoint]
    val subscriptionsEndpoint    = mock[TestSubscriptionEndpoint]
    val routes = new MicroserviceRoutes[IO](
      eventCreationEndpoint,
      latestEventsEndpoint,
      processingStatusEndpoint,
      eventsPatchingEndpoint,
      statusChangeEndpoint,
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
  class TestStatusChangeEndpoint(updateCommandsRunner: StatusUpdatesRunner[IO],
                                 waitingEventsGauge:   LabeledGauge[IO, projects.Path],
                                 underProcessingGauge: LabeledGauge[IO, projects.Path],
                                 logger:               Logger[IO])
      extends StatusChangeEndpoint[IO](updateCommandsRunner, waitingEventsGauge, underProcessingGauge, logger)
}
