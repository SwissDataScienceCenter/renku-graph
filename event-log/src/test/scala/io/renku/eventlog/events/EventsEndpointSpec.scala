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

package io.renku.eventlog.events

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.{pagingRequests, pagingResponses}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.{EventId, EventProcessingTime, EventStatus}
import ch.datascience.http.ErrorMessage
import ch.datascience.http.ErrorMessage._
import ch.datascience.http.rest.paging.PagingRequest.Decoders.{page, perPage}
import ch.datascience.http.rest.paging.model.Total
import ch.datascience.http.rest.paging.{PagingHeaders, PagingResponse}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import io.circe.Decoder
import io.renku.eventlog.events.EventsEndpoint._
import io.renku.eventlog.events.Generators.eventInfos
import io.renku.eventlog.{EventDate, EventMessage, ExecutionDate}
import org.http4s.EntityDecoder
import org.http4s.MediaType._
import org.http4s.Status.{InternalServerError, Ok}
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class EventsEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "findEvents" should {

    s"$Ok with an empty array if there are no events found" in new TestCase {
      val pagingResponse = PagingResponse.from[Try, EventInfo](Nil, pagingRequest, Total(0)).fold(throw _, identity)
      (eventsFinder.findEvents _)
        .expects(projectPath, pagingRequest)
        .returning(pagingResponse.pure[IO])

      val response = endpoint.findEvents(projectPath, pagingRequest).unsafeRunSync()

      response.status                              shouldBe Ok
      response.contentType                         shouldBe Some(`Content-Type`(application.json))
      response.as[List[EventInfo]].unsafeRunSync() shouldBe Nil
      response.headers.toList                        should contain allElementsOf PagingHeaders.from[IO, EventLogUrl](pagingResponse)
    }

    s"$Ok with array of events if there are events found" in new TestCase {

      val pagingResponse = pagingResponses(eventInfos).generateOne

      (eventsFinder.findEvents _)
        .expects(projectPath, pagingRequest)
        .returning(pagingResponse.pure[IO])

      val response = endpoint.findEvents(projectPath, pagingRequest).unsafeRunSync()

      response.status                              shouldBe Ok
      response.contentType                         shouldBe Some(`Content-Type`(application.json))
      response.as[List[EventInfo]].unsafeRunSync() shouldBe pagingResponse.results
      response.headers.toList                        should contain allElementsOf PagingHeaders.from[IO, EventLogUrl](pagingResponse)
    }

    s"$InternalServerError when an error happens while fetching the events" in new TestCase {
      val exception = exceptions.generateOne
      (eventsFinder.findEvents _)
        .expects(projectPath, pagingRequest)
        .returning(exception.raiseError[IO, PagingResponse[EventInfo]])

      val response = endpoint.findEvents(projectPath, pagingRequest).unsafeRunSync()

      response.status                           shouldBe InternalServerError
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(exception.getMessage)

      logger.loggedOnly(Error(s"Finding events for project '$projectPath' failed", exception))
    }
  }

  private trait TestCase {
    val projectPath   = projectPaths.generateOne
    val pagingRequest = pagingRequests.generateOne

    private val eventLogUrl: EventLogUrl = httpUrls().generateAs(EventLogUrl)
    implicit val resourceUrl: EventLogUrl =
      (eventLogUrl / "events") ? (page.parameterName -> pagingRequest.page) & (perPage.parameterName -> pagingRequest.perPage)

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val eventsFinder = mock[EventsFinder[IO]]
    val endpoint     = new EventsEndpointImpl[IO](eventsFinder, eventLogUrl)
  }

  private implicit val eventInfoDecoder: Decoder[EventInfo] = cursor =>
    for {
      id              <- cursor.downField("id").as[EventId]
      status          <- cursor.downField("status").as[EventStatus]
      eventDate       <- cursor.downField("date").as[EventDate]
      executionDate   <- cursor.downField("executionDate").as[ExecutionDate]
      maybeMessage    <- cursor.downField("message").as[Option[EventMessage]]
      processingTimes <- cursor.downField("processingTimes").as[List[StatusProcessingTime]]
    } yield EventInfo(id, status, eventDate, executionDate, maybeMessage, processingTimes)

  private implicit val statusProcessingTimeDecoder: Decoder[StatusProcessingTime] = cursor =>
    for {
      status         <- cursor.downField("status").as[EventStatus]
      processingTime <- cursor.downField("processingTime").as[EventProcessingTime]
    } yield StatusProcessingTime(status, processingTime)

  private implicit val entityDecoder: EntityDecoder[IO, List[EventInfo]] =
    org.http4s.circe.jsonOf[IO, List[EventInfo]]
}
