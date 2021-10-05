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

package io.renku.eventlog

import cats.effect.{Clock, IO}
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.pagingRequests
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonEmptyStrings
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventStatuses}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.http.ErrorMessage.ErrorMessage
import ch.datascience.http.InfoMessage.InfoMessage
import ch.datascience.http.rest.paging.PagingRequest
import ch.datascience.http.server.EndpointTester._
import ch.datascience.http.{ErrorMessage, InfoMessage}
import ch.datascience.interpreters.TestRoutesMetrics
import io.renku.eventlog.eventdetails.EventDetailsEndpoint
import io.renku.eventlog.events.{EventEndpoint, EventsEndpoint}
import io.renku.eventlog.processingstatus.ProcessingStatusEndpoint
import io.renku.eventlog.subscriptions.SubscriptionsEndpoint
import org.http4s.MediaType.application
import org.http4s.Method.{GET, POST}
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext
import scala.language.reflectiveCalls

class MicroserviceRoutesSpec extends AnyWordSpec with MockFactory with should.Matchers with TableDrivenPropertyChecks {

  "routes" should {

    "define a GET /events - case with project-path only" in new TestCase {
      val projectPath = projectPaths.generateOne

      val request = Request[IO](method = GET, uri"events".withQueryParam("project-path", projectPath.value))

      (eventsEndpoint.findEvents _)
        .expects(EventsEndpoint.Request.ProjectEvents(projectPath, None, PagingRequest.default))
        .returning(Response[IO](Ok).pure[IO])

      routes.call(request).status shouldBe Ok
    }

    "define a GET /events - case with status only" in new TestCase {
      val eventStatus = eventStatuses.generateOne

      val request = Request[IO](method = GET, uri"events".withQueryParam("status", eventStatus.value))

      (eventsEndpoint.findEvents _)
        .expects(EventsEndpoint.Request.EventsWithStatus(eventStatus, PagingRequest.default))
        .returning(Response[IO](Ok).pure[IO])

      routes.call(request).status shouldBe Ok
    }

    "define a GET /events - case with project-path and status" in new TestCase {
      val projectPath   = projectPaths.generateOne
      val eventStatus   = eventStatuses.generateOne
      val pagingRequest = pagingRequests.generateOne

      val request = Request[IO](
        method = GET,
        uri"events"
          .withQueryParam("project-path", projectPath.value)
          .withQueryParam("status", eventStatus.value)
          .withQueryParam("page", pagingRequest.page.value)
          .withQueryParam("per_page", pagingRequest.perPage.value)
      )

      (eventsEndpoint.findEvents _)
        .expects(EventsEndpoint.Request.ProjectEvents(projectPath, Some(eventStatus), pagingRequest))
        .returning(Response[IO](Ok).pure[IO])

      routes.call(request).status shouldBe Ok
    }

    Set(
      uri"events".withQueryParam("project-path", nonEmptyStrings().generateOne),
      uri"events".withQueryParam("status", nonEmptyStrings().generateOne),
      uri"events"
        .withQueryParam("status", eventStatuses.generateOne.show)
        .withQueryParam("page", nonEmptyStrings().generateOne),
      uri"events"
        .withQueryParam("status", eventStatuses.generateOne.show)
        .withQueryParam("per_page", nonEmptyStrings().generateOne)
    ) foreach { uri =>
      s"define a GET $uri returning $BadRequest for invalid project path" in new TestCase {
        routes.call(Request[IO](method = GET, uri)).status shouldBe BadRequest
      }
    }

    "define a GET /events not finding the endpoint when no project-path or status parameter is present" in new TestCase {
      routes.call(Request[IO](method = GET, uri"events")).status shouldBe NotFound
    }

    "define a GET /events/:event-id/:project-id endpoint" in new TestCase {
      val eventId = compoundEventIds.generateOne

      val request = Request[IO](
        method = GET,
        uri"events" / eventId.id.toString / eventId.projectId.toString
      )

      (eventDetailsEndpoint.getDetails _).expects(eventId).returning(Response[IO](Ok).pure[IO])

      val response = routes.call(request)

      response.status shouldBe Ok
    }

    "define a POST /events endpoint" in new TestCase {
      val request        = Request[IO](POST, uri"events")
      val expectedStatus = Gen.oneOf(Accepted, BadRequest, InternalServerError, TooManyRequests).generateOne
      (eventEndpoint.processEvent _).expects(request).returning(Response[IO](expectedStatus).pure[IO])

      val response = routes.call(request)

      response.status shouldBe expectedStatus
    }

    "define a GET /metrics endpoint returning OK with some prometheus metrics" in new TestCase {
      val response = routes.call(
        Request(GET, uri"metrics")
      )

      response.status     shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }

    "define a GET /ping endpoint returning OK with 'pong' body" in new TestCase {
      val response = routes.call(Request(GET, uri"ping"))

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }

    "define a GET /processing-status?project-id=:id endpoint" in new TestCase {
      val projectId = projectIds.generateOne

      val request = Request[IO](GET, uri"processing-status".withQueryParam("project-id", projectId.toString))
      (processingStatusEndpoint.findProcessingStatus _).expects(projectId).returning(Response[IO](Ok).pure[IO])

      val response = routes.call(request)

      response.status shouldBe Ok
    }

    "define a GET /processing-status?project-id=:id endpoint " +
      s"returning $NotFound if no project-id parameter is given" in new TestCase {

        val request = Request[IO](GET, uri"processing-status")

        val response = routes.call(request)

        response.status            shouldBe NotFound
        response.contentType       shouldBe Some(`Content-Type`(application.json))
        response.body[InfoMessage] shouldBe InfoMessage("No 'project-id' parameter")
      }

    "define a GET /processing-status?project-id=:id endpoint " +
      s"returning $BadRequest if illegal project-id parameter value is given" in new TestCase {
        val request = Request[IO](GET, uri"processing-status".withQueryParam("project-id", "non int value"))

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
    val eventEndpoint            = mock[EventEndpoint[IO]]
    val eventsEndpoint           = mock[EventsEndpoint[IO]]
    val processingStatusEndpoint = mock[ProcessingStatusEndpoint[IO]]
    val subscriptionsEndpoint    = mock[SubscriptionsEndpoint[IO]]
    val eventDetailsEndpoint     = mock[EventDetailsEndpoint[IO]]
    val routesMetrics            = TestRoutesMetrics()
    val routes = new MicroserviceRoutes[IO](
      eventEndpoint,
      eventsEndpoint,
      processingStatusEndpoint,
      subscriptionsEndpoint,
      eventDetailsEndpoint,
      routesMetrics
    ).routes.map(_.or(notAvailableResponse))
  }
}
