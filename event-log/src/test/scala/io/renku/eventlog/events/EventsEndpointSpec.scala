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

import EventsEndpoint._
import Generators._
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.{EventId, EventProcessingTime, EventStatus}
import ch.datascience.http.ErrorMessage
import ch.datascience.http.ErrorMessage.ErrorMessage
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import io.circe.Decoder
import io.renku.eventlog.EventMessage
import org.http4s.EntityDecoder
import org.http4s.MediaType._
import org.http4s.Status.{InternalServerError, Ok}
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventsEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "findEvents" should {

    s"$Ok with an empty array if there are no events found" in new TestCase {
      (eventsFinder.findEvents _)
        .expects(projectPath)
        .returning(Nil.pure[IO])

      val response = endpoint.findEvents(projectPath).unsafeRunSync()

      response.status                              shouldBe Ok
      response.contentType                         shouldBe Some(`Content-Type`(application.json))
      response.as[List[EventInfo]].unsafeRunSync() shouldBe Nil
    }

    s"$Ok with array of events if there are events found" in new TestCase {

      val eventInfoList = eventInfos.generateList()
      (eventsFinder.findEvents _)
        .expects(projectPath)
        .returning(eventInfoList.pure[IO])

      val response = endpoint.findEvents(projectPath).unsafeRunSync()

      response.status                              shouldBe Ok
      response.contentType                         shouldBe Some(`Content-Type`(application.json))
      response.as[List[EventInfo]].unsafeRunSync() shouldBe eventInfoList
    }

    s"$InternalServerError when an error happens while fetching the events" in new TestCase {
      val exception = exceptions.generateOne
      (eventsFinder.findEvents _)
        .expects(projectPath)
        .returning(exception.raiseError[IO, List[EventInfo]])

      val response = endpoint.findEvents(projectPath).unsafeRunSync()

      response.status                           shouldBe InternalServerError
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(exception.getMessage)

      logger.loggedOnly(Error(s"Finding events for project '$projectPath' failed", exception))
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val eventsFinder = mock[EventsFinder[IO]]
    val endpoint     = new EventsEndpointImpl[IO](eventsFinder)
  }

  private implicit val eventInfoDecoder: Decoder[EventInfo] = cursor =>
    for {
      id              <- cursor.downField("id").as[EventId]
      status          <- cursor.downField("status").as[EventStatus]
      maybeMessage    <- cursor.downField("message").as[Option[EventMessage]]
      processingTimes <- cursor.downField("processingTimes").as[List[StatusProcessingTime]]
    } yield EventInfo(id, status, maybeMessage, processingTimes)

  private implicit val statusProcessingTimeDecoder: Decoder[StatusProcessingTime] = cursor =>
    for {
      status         <- cursor.downField("status").as[EventStatus]
      processingTime <- cursor.downField("processingTime").as[EventProcessingTime]
    } yield StatusProcessingTime(status, processingTime)

  private implicit val entityDecoder: EntityDecoder[IO, List[EventInfo]] =
    org.http4s.circe.jsonOf[IO, List[EventInfo]]
}
