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

package io.renku.eventlog

import EventContentGenerators._
import cats.Show
import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.renku.eventlog.eventdetails.EventDetailsEndpoint
import io.renku.eventlog.events.producers.SubscriptionsEndpoint
import io.renku.eventlog.events.{EventEndpoint, EventsEndpoint}
import io.renku.eventlog.processingstatus.ProcessingStatusEndpoint
import io.renku.generators.CommonGraphGenerators.{httpStatuses, pages, perPages, sortingDirections}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{jsons, nonEmptyStrings}
import io.renku.graph.model.EventsGenerators.{compoundEventIds, eventStatuses}
import io.renku.graph.model.GraphModelGenerators._
import io.renku.http.ErrorMessage.ErrorMessage
import io.renku.http.InfoMessage.InfoMessage
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.server.EndpointTester._
import io.renku.http.server.version
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.interpreters.TestRoutesMetrics
import io.renku.testtools.IOSpec
import io.renku.tinytypes.InstantTinyType
import org.http4s.MediaType.application
import org.http4s.Method.{GET, POST}
import org.http4s.QueryParamEncoder._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.language.reflectiveCalls

class MicroserviceRoutesSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with should.Matchers
    with TableDrivenPropertyChecks {

  "GET /events" should {
    import EventsEndpoint.Criteria
    import EventsEndpoint.Criteria.Sorting._
    import EventsEndpoint.Criteria._

    forAll {
      Table(
        "uri" -> "criteria",
        projectPaths
          .map(path =>
            uri"/events" +? ("project-path" -> path.value) -> Criteria(
              Filters.ProjectEvents(path, maybeStatus = None, maybeDates = None)
            )
          )
          .generateOne,
        (projectPaths -> eventStatuses).mapN { case (path, status) =>
          uri"/events" +? ("project-path" -> path.value) +? ("status" -> status.value) -> Criteria(
            Filters.ProjectEvents(path, Some(status), maybeDates = None)
          )
        }.generateOne,
        (projectPaths -> eventDates).mapN { case (path, since) =>
          uri"/events" +? ("project-path" -> path.value) +? ("since" -> since) -> Criteria(
            Filters.ProjectEvents(path, None, Filters.EventsSince(since).some)
          )
        }.generateOne,
        (projectPaths -> eventDates).mapN { case (path, until) =>
          uri"/events" +? ("project-path" -> path.value) +? ("until" -> until) -> Criteria(
            Filters.ProjectEvents(path, None, Filters.EventsUntil(until).some)
          )
        }.generateOne,
        (projectPaths, eventDates, eventDates).mapN { case (path, since, until) =>
          uri"/events" +? ("project-path" -> path.value) +? ("since" -> since) +? ("until" -> until) -> Criteria(
            Filters.ProjectEvents(
              path,
              maybeStatus = None,
              Filters.EventsSinceAndUntil(Filters.EventsSince(since), Filters.EventsUntil(until)).some
            )
          )
        }.generateOne,
        eventStatuses
          .map(status =>
            uri"/events" +? ("status" -> status.value) -> Criteria(Filters.EventsWithStatus(status, maybeDates = None))
          )
          .generateOne,
        (eventStatuses -> eventDates).mapN { case (status, since) =>
          uri"/events" +? ("status" -> status.value) +? ("since" -> since) -> Criteria(
            Filters.EventsWithStatus(status, Filters.EventsSince(since).some)
          )
        }.generateOne,
        (eventStatuses -> eventDates).mapN { case (status, until) =>
          uri"/events" +? ("status" -> status.value) +? ("until" -> until) -> Criteria(
            Filters.EventsWithStatus(status, Filters.EventsUntil(until).some)
          )
        }.generateOne,
        (eventStatuses, eventDates, eventDates).mapN { case (status, since, until) =>
          uri"/events" +? ("status" -> status.value) +? ("since" -> since) +? ("until" -> until) -> Criteria(
            Filters.EventsWithStatus(
              status,
              Filters.EventsSinceAndUntil(Filters.EventsSince(since), Filters.EventsUntil(until)).some
            )
          )
        }.generateOne,
        eventDates
          .map(since => uri"/events" +? ("since" -> since) -> Criteria(Filters.EventsSince(since)))
          .generateOne,
        eventDates
          .map(until => uri"/events" +? ("until" -> until) -> Criteria(Filters.EventsUntil(until)))
          .generateOne,
        (eventDates, eventDates)
          .mapN((since, until) =>
            uri"/events" +? ("since" -> since) +? ("until" -> until) -> Criteria(
              Filters.EventsSinceAndUntil(Filters.EventsSince(since), Filters.EventsUntil(until))
            )
          )
          .generateOne,
        (eventStatuses -> sortingDirections).mapN { (status, dir) =>
          uri"/events" +? ("status" -> status.value) +? ("sort" -> s"eventDate:$dir") -> Criteria(
            Filters.EventsWithStatus(status, maybeDates = None),
            Sorting.By(EventDate, dir)
          )
        }.generateOne,
        (eventStatuses -> pages).mapN { (status, page) =>
          uri"/events" +? ("status" -> status.value) +? ("page" -> page.show) -> Criteria(
            Filters.EventsWithStatus(status, maybeDates = None),
            paging = PagingRequest.default.copy(page = page)
          )
        }.generateOne,
        (eventStatuses -> perPages).mapN { (status, perPage) =>
          uri"/events" +? ("status" -> status.value) +? ("per_page" -> perPage.show) -> Criteria(
            Filters.EventsWithStatus(status, maybeDates = None),
            paging = PagingRequest.default.copy(perPage = perPage)
          )
        }.generateOne
      )
    } { (uri, criteria) =>
      s"read the query parameters from $uri, pass them to the endpoint and return received response" in new TestCase
        with IsNotMigrating {
        val request = Request[IO](Method.GET, uri)

        val responseBody = jsons.generateOne
        (eventsEndpoint.findEvents _)
          .expects(criteria, request)
          .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

        val response = routes.call(request)

        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(application.json))
        response.body[Json]  shouldBe responseBody
      }
    }

    Set(
      uri"/events".withQueryParam("project-path", nonEmptyStrings().generateOne),
      uri"/events".withQueryParam("status", nonEmptyStrings().generateOne),
      uri"/events"
        .withQueryParam("status", eventStatuses.generateOne.show)
        .withQueryParam("sort", nonEmptyStrings().generateOne),
      uri"/events".withQueryParam("since", nonEmptyStrings().generateOne),
      uri"/events".withQueryParam("until", nonEmptyStrings().generateOne),
      uri"/events"
        .withQueryParam("status", eventStatuses.generateOne.show)
        .withQueryParam("page", nonEmptyStrings().generateOne),
      uri"/events"
        .withQueryParam("status", eventStatuses.generateOne.show)
        .withQueryParam("per_page", nonEmptyStrings().generateOne)
    ) foreach { uri =>
      s"return $BadRequest when invalid $uri uri" in new TestCase with IsNotMigrating {
        routes.call(Request[IO](method = GET, uri)).status shouldBe BadRequest
      }
    }

    s"return $NotFound if no project-path or status parameter is present" in new TestCase with IsNotMigrating {
      routes.call(Request[IO](method = GET, uri"/events")).status shouldBe NotFound
    }
  }

  "GET /events/:event-id/:project-id" should {

    s"find the event details and return them as with $Ok" in new TestCase with IsNotMigrating {
      val eventId = compoundEventIds.generateOne

      val request = Request[IO](
        method = GET,
        uri"/events" / eventId.id.toString / eventId.projectId.toString
      )

      val responseBody = jsons.generateOne
      (eventDetailsEndpoint.getDetails _).expects(eventId).returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

      val response = routes.call(request)

      response.status      shouldBe Ok
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.body[Json]  shouldBe responseBody
    }
  }

  "POST /events" should {

    "process the event and return the status returned from the endpoint" in new TestCase with IsNotMigrating {
      val request        = Request[IO](POST, uri"/events")
      val expectedStatus = Gen.oneOf(Accepted, BadRequest, InternalServerError, TooManyRequests).generateOne
      (eventEndpoint.processEvent _).expects(request).returning(Response[IO](expectedStatus).pure[IO])

      routes.call(request).status shouldBe expectedStatus
    }
  }

  "GET /metrics" should {

    s"return $Ok with some prometheus metrics" in new TestCase {
      val response = routes.call(Request(GET, uri"/metrics"))

      response.status     shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }
  }

  "GET /ping" should {

    s"return $Ok with 'pong' body" in new TestCase {
      val response = routes.call(Request(GET, uri"/ping"))

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }
  }

  "GET /version" should {
    "return response from the version endpoint" in new TestCase {
      routes.call(Request(GET, uri"/version")).status shouldBe versionEndpointResponse.status
    }
  }

  "GET /processing-status?project-id=:id" should {

    s"find processing status and return it with $Ok" in new TestCase with IsNotMigrating {
      val projectId = projectIds.generateOne

      val request = Request[IO](GET, uri"/processing-status".withQueryParam("project-id", projectId.toString))
      (processingStatusEndpoint.findProcessingStatus _).expects(projectId).returning(Response[IO](Ok).pure[IO])

      routes.call(request).status shouldBe Ok
    }

    s"return $NotFound if no project-id parameter is given" in new TestCase with IsNotMigrating {

      val request = Request[IO](GET, uri"/processing-status")

      val response = routes.call(request)

      response.status            shouldBe NotFound
      response.contentType       shouldBe Some(`Content-Type`(application.json))
      response.body[InfoMessage] shouldBe InfoMessage("No 'project-id' parameter")
    }

    s"return $BadRequest if illegal project-id parameter value is given" in new TestCase with IsNotMigrating {
      val request = Request[IO](GET, uri"/processing-status".withQueryParam("project-id", "non int value"))

      val response = routes.call(request)

      response.status             shouldBe BadRequest
      response.contentType        shouldBe Some(`Content-Type`(application.json))
      response.body[ErrorMessage] shouldBe ErrorMessage("'project-id' parameter with invalid value")
    }
  }

  "POST /subscriptions" should {

    s"add a subscription and return $Accepted" in new TestCase with IsNotMigrating {
      val request = Request[IO](POST, uri"/subscriptions")

      (subscriptionsEndpoint.addSubscription _).expects(request).returning(Response[IO](Accepted).pure[IO])

      routes.call(request).status shouldBe Accepted
    }
  }

  "GET /migration-status" should {

    "return OK with \"isMigrating\": false in the json body" in new TestCase with IsNotMigrating {
      val response = routes.call(Request(GET, uri"/migration-status"))

      response.status     shouldBe Ok
      response.body[Json] shouldBe json"""{"isMigrating": false}"""
    }
  }

  "all endpoints except /ping and /version" should {

    s"return $ServiceUnavailable if migration is happening" in new TestCase {
      (() => isMigrating.get)
        .expects()
        .returning(true.pure[IO])

      val request = Request[IO](GET, uri"/events".withQueryParam("project-path", projectPaths.generateOne.value))
      routes.call(request).status shouldBe ServiceUnavailable

      routes.call(Request(GET, uri"/ping")).status    shouldBe Ok
      routes.call(Request(GET, uri"/version")).status shouldBe versionEndpointResponse.status
    }
  }

  private trait TestCase {
    val eventEndpoint            = mock[EventEndpoint[IO]]
    val eventsEndpoint           = mock[EventsEndpoint[IO]]
    val processingStatusEndpoint = mock[ProcessingStatusEndpoint[IO]]
    val subscriptionsEndpoint    = mock[SubscriptionsEndpoint[IO]]
    val eventDetailsEndpoint     = mock[EventDetailsEndpoint[IO]]
    val routesMetrics            = TestRoutesMetrics()
    val isMigrating              = mock[Ref[IO, Boolean]]
    val versionRoutes            = mock[version.Routes[IO]]
    val routes = new MicroserviceRoutes[IO](
      eventEndpoint,
      eventsEndpoint,
      processingStatusEndpoint,
      subscriptionsEndpoint,
      eventDetailsEndpoint,
      routesMetrics,
      isMigrating,
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

  private implicit def instantQueryParamEncoder[TT <: InstantTinyType](implicit
      show: Show[TT]
  ): QueryParamEncoder[TT] = fromShow(show)

  private trait IsNotMigrating {
    self: TestCase =>

    (() => isMigrating.get)
      .expects()
      .returning(IO(false))
  }
}
