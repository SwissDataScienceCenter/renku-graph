/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

import cats.Show
import cats.effect.{IO, Ref}
import cats.syntax.all._
import eventdetails.EventDetailsEndpoint
import eventpayload.EventPayloadEndpoint
import events.producers.SubscriptionsEndpoint
import events.{EventEndpoint, EventsEndpoint}
import io.circe.Json
import io.circe.literal._
import io.renku.eventlog.status.StatusEndpoint
import io.renku.generators.CommonGraphGenerators.{httpStatuses, pages, perPages, sortingDirections}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{jsons, nonEmptyStrings}
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators.{compoundEventIds, eventIds, eventStatuses}
import io.renku.graph.model.GraphModelGenerators._
import io.renku.http.rest.Sorting
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.server.EndpointTester._
import io.renku.http.server.version
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
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scodec.bits.ByteVector

import scala.language.reflectiveCalls

class MicroserviceRoutesSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with should.Matchers
    with TableDrivenPropertyChecks {

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  "GET /events" should {
    import EventsEndpoint.Criteria
    import EventsEndpoint.Criteria.Sort._
    import EventsEndpoint.Criteria._

    forAll {
      Table(
        "uri" -> "criteria",
        projectSlugs
          .map(slug =>
            uri"/events" +? ("project-slug" -> slug.value) -> Criteria(
              Filters.ProjectEvents(slug, maybeStatus = None, maybeDates = None)
            )
          )
          .generateOne,
        projectIds
          .map(id =>
            uri"/events" +? ("project-id" -> id.value) -> Criteria(
              Filters.ProjectEvents(id, maybeStatus = None, maybeDates = None)
            )
          )
          .generateOne,
        (projectSlugs -> eventStatuses).mapN { case (slug, status) =>
          uri"/events" +? ("project-slug" -> slug.value) +? ("status" -> status.value) -> Criteria(
            Filters.ProjectEvents(slug, Some(status), maybeDates = None)
          )
        }.generateOne,
        (projectSlugs -> eventDates).mapN { case (slug, since) =>
          uri"/events" +? ("project-slug" -> slug.value) +? ("since" -> since) -> Criteria(
            Filters.ProjectEvents(slug, None, Filters.EventsSince(since).some)
          )
        }.generateOne,
        (projectSlugs -> eventDates).mapN { case (slug, until) =>
          uri"/events" +? ("project-slug" -> slug.value) +? ("until" -> until) -> Criteria(
            Filters.ProjectEvents(slug, None, Filters.EventsUntil(until).some)
          )
        }.generateOne,
        (projectSlugs, eventDates, eventDates).mapN { case (slug, since, until) =>
          uri"/events" +? ("project-slug" -> slug.value) +? ("since" -> since) +? ("until" -> until) -> Criteria(
            Filters.ProjectEvents(
              slug,
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
            Sorting(Sort.By(EventDate, dir))
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
      uri"/events".withQueryParam("project-slug", nonEmptyStrings().generateOne),
      uri"/events".withQueryParam("project-id", nonEmptyStrings().generateOne),
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

    s"return $NotFound if no project-slug or status parameter is present" in new TestCase with IsNotMigrating {
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

  "GET /status" should {

    "return response from the status endpoint" in new TestCase with IsNotMigrating {

      val response = Response[IO](status = httpStatuses.generateOne)
      (() => statusEndpoint.`GET /status`).expects().returning(response.pure[IO])

      routes.call(Request(GET, uri"/status")).status shouldBe response.status
    }

    s"return $ServiceUnavailable when migration is running" in new TestCase {
      givenMigrationIsRunning
      routes.call(Request(GET, uri"/status")).status shouldBe ServiceUnavailable
    }
  }

  "POST /subscriptions" should {

    s"add a subscription and return $Accepted" in new TestCase with IsNotMigrating {
      val request = Request[IO](POST, uri"/subscriptions")

      (subscriptionsEndpoint.addSubscription _).expects(request).returning(Response[IO](Accepted).pure[IO])

      routes.call(request).status shouldBe Accepted
    }
  }

  "GET /version" should {
    "return response from the version endpoint" in new TestCase {
      routes.call(Request(GET, uri"/version")).status shouldBe versionEndpointResponse.status
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

      givenMigrationIsRunning

      val request = Request[IO](GET, uri"/events".withQueryParam("project-slug", projectSlugs.generateOne.value))
      routes.call(request).status shouldBe ServiceUnavailable

      routes.call(Request(GET, uri"/ping")).status    shouldBe Ok
      routes.call(Request(GET, uri"/version")).status shouldBe versionEndpointResponse.status
    }
  }

  "GET /events/:event-id/:project-slug/payload" should {

    s"find event payload and return $Ok" in new TestCase with IsNotMigrating {
      val eventId     = eventIds.generateOne
      val projectSlug = projectSlugs.generateOne

      val request = Request[IO](
        method = GET,
        uri"/events" / eventId.toString / projectSlug.toString / "payload"
      )

      val someData = ByteVector(0, 10, -10, 5)
      (eventPayloadEndpoint.getEventPayload _)
        .expects(eventId, projectSlug)
        .returning(Response[IO](Ok).withEntity(someData).pure[IO])

      val response = routes.call(request)

      response.status           shouldBe Ok
      response.body[ByteVector] shouldBe someData
    }
  }

  private trait TestCase {
    val eventEndpoint         = mock[EventEndpoint[IO]]
    val eventsEndpoint        = mock[EventsEndpoint[IO]]
    val subscriptionsEndpoint = mock[SubscriptionsEndpoint[IO]]
    val eventDetailsEndpoint  = mock[EventDetailsEndpoint[IO]]
    val eventPayloadEndpoint  = mock[EventPayloadEndpoint[IO]]
    val statusEndpoint        = mock[StatusEndpoint[IO]]
    val routesMetrics         = TestRoutesMetrics()
    val isMigrating           = mock[Ref[IO, Boolean]]
    val versionRoutes         = mock[version.Routes[IO]]
    def routes = new MicroserviceRoutes[IO](eventEndpoint,
                                            eventsEndpoint,
                                            subscriptionsEndpoint,
                                            eventDetailsEndpoint,
                                            eventPayloadEndpoint,
                                            statusEndpoint,
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

    def givenMigrationIsRunning =
      (() => isMigrating.get)
        .expects()
        .returning(true.pure[IO])
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
