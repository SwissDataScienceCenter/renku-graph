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

package io.renku.eventlog.events

import cats.effect.IO
import cats.syntax.all._
import io.circe.Decoder
import io.renku.eventlog.events.EventsEndpoint._
import io.renku.eventlog.events.Generators.eventInfos
import io.renku.eventlog.{EventDate, EventMessage, ExecutionDate}
import io.renku.generators.CommonGraphGenerators.{pagingRequests, pagingResponses, sortBys}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.EventsGenerators.eventStatuses
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.{EventId, EventProcessingTime, EventStatus}
import io.renku.graph.model.projects
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import io.renku.http.rest.paging.model.Total
import io.renku.http.rest.paging.{PagingHeaders, PagingResponse}
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.http4s.MediaType._
import org.http4s.Method.GET
import org.http4s.Status.{InternalServerError, Ok}
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.http4s.{EntityDecoder, Request}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class EventsEndpointSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "findEvents" should {

    s"$Ok with an empty array if there are no events found" in new TestCase {

      val pagingResponse =
        PagingResponse.from[Try, EventInfo](Nil, criteria.paging, Total(0)).fold(throw _, identity)
      (eventsFinder.findEvents _)
        .expects(criteria)
        .returning(pagingResponse.pure[IO])

      val response = endpoint.findEvents(criteria, request).unsafeRunSync()

      response.status                              shouldBe Ok
      response.contentType                         shouldBe Some(`Content-Type`(application.json))
      response.as[List[EventInfo]].unsafeRunSync() shouldBe Nil
      response.headers.headers should contain allElementsOf PagingHeaders.from[IO, EventLogUrl](pagingResponse)
    }

    s"$Ok with array of events if there are events found" in new TestCase {

      val pagingResponse = pagingResponses(eventInfos()).generateOne

      (eventsFinder.findEvents _)
        .expects(criteria)
        .returning(pagingResponse.pure[IO])

      val response = endpoint.findEvents(criteria, request).unsafeRunSync()

      response.status                              shouldBe Ok
      response.contentType                         shouldBe Some(`Content-Type`(application.json))
      response.as[List[EventInfo]].unsafeRunSync() shouldBe pagingResponse.results
      response.headers.headers should contain allElementsOf PagingHeaders.from[IO, EventLogUrl](pagingResponse)
    }

    s"$InternalServerError when an error happens while fetching the events" in new TestCase {

      val exception = exceptions.generateOne
      (eventsFinder.findEvents _)
        .expects(criteria)
        .returning(exception.raiseError[IO, PagingResponse[EventInfo]])

      val response = endpoint.findEvents(criteria, request).unsafeRunSync()

      response.status                           shouldBe InternalServerError
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(exception.getMessage)

      logger.loggedOnly(Error(show"Finding events for '${request.uri}' failed", exception))
    }
  }

  private trait TestCase {
    import EventsEndpoint.Criteria._

    val criterias: Gen[EventsEndpoint.Criteria] = Gen.oneOf(
      for {
        projectPath <- projectPaths
        maybeStatus <- eventStatuses.toGeneratorOfOptions
        sorting     <- sortBys(Criteria.Sorting)
        paging      <- pagingRequests
      } yield Criteria(Filters.ProjectEvents(projectPath, maybeStatus), sorting, paging),
      for {
        status  <- eventStatuses
        sorting <- sortBys(Criteria.Sorting)
        paging  <- pagingRequests
      } yield Criteria(Filters.EventsWithStatus(status), sorting, paging)
    )

    val criteria = criterias.generateOne
    val request  = Request[IO](GET, uri"/events")

    val eventLogUrl:          EventLogUrl = httpUrls().generateAs(EventLogUrl)
    implicit val resourceUrl: EventLogUrl = EventLogUrl(show"$eventLogUrl${request.uri}")

    val eventsFinder = mock[EventsFinder[IO]]
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val endpoint = new EventsEndpointImpl[IO](eventsFinder, eventLogUrl)
  }

  private implicit val eventInfoDecoder: Decoder[EventInfo] = cursor =>
    for {
      id              <- cursor.downField("id").as[EventId]
      projectPath     <- cursor.downField("projectPath").as[projects.Path]
      status          <- cursor.downField("status").as[EventStatus]
      eventDate       <- cursor.downField("date").as[EventDate]
      executionDate   <- cursor.downField("executionDate").as[ExecutionDate]
      maybeMessage    <- cursor.downField("message").as[Option[EventMessage]]
      processingTimes <- cursor.downField("processingTimes").as[List[StatusProcessingTime]]
    } yield EventInfo(id, projectPath, status, eventDate, executionDate, maybeMessage, processingTimes)

  private implicit val statusProcessingTimeDecoder: Decoder[StatusProcessingTime] = cursor =>
    for {
      status         <- cursor.downField("status").as[EventStatus]
      processingTime <- cursor.downField("processingTime").as[EventProcessingTime]
    } yield StatusProcessingTime(status, processingTime)

  private implicit val entityDecoder: EntityDecoder[IO, List[EventInfo]] = org.http4s.circe.jsonOf[IO, List[EventInfo]]
}
