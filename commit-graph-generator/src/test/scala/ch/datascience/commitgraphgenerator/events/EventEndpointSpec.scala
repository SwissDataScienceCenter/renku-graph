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

package ch.datascience.commitgraphgenerator.events

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.events.consumers.ConsumersModelGenerators._
import ch.datascience.events.consumers._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.ErrorMessage.ErrorMessage
import ch.datascience.http.InfoMessage._
import ch.datascience.http.server.EndpointTester._
import ch.datascience.http.{ErrorMessage, InfoMessage}
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.http4s.multipart.{Multipart, Part}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "processEvent" should {

    s"$BadRequest if the request is not a multipart request" in new TestCase {

      val response = processEvent(Request()).unsafeRunSync()

      response.status                          shouldBe BadRequest
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage("Not multipart request")
    }

    s"$BadRequest if there is no event part in the request" in new TestCase {

      val multipart =
        Multipart[IO](Vector(Part.formData[IO](nonEmptyStrings().generateOne, nonEmptyStrings().generateOne)))

      val response = processEvent(Request().withEntity(multipart).withHeaders(multipart.headers)).unsafeRunSync()

      response.status                          shouldBe BadRequest
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage("Missing event part")
    }

    s"$BadRequest if there the event part in the request is malformed" in new TestCase {

      val multipart = Multipart[IO](Vector(Part.formData[IO]("event", "")))

      val response = processEvent(Request().withEntity(multipart).withHeaders(multipart.headers)).unsafeRunSync()

      response.status                          shouldBe BadRequest
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage("Malformed event body")
    }

    s"$Accepted if one of the handlers accepts the given payload" in new TestCase {

      (subscriptionsRegistry.handle _)
        .expects(requestContent)
        .returning(EventSchedulingResult.Accepted.pure[IO])

      val response = processEvent(request).unsafeRunSync()

      response.status                          shouldBe Accepted
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event accepted")
    }

    s"$BadRequest if none of the handlers supports the given payload" in new TestCase {

      (subscriptionsRegistry.handle _)
        .expects(requestContent)
        .returning(EventSchedulingResult.UnsupportedEventType.pure[IO])

      val response = processEvent(request).unsafeRunSync()

      response.status                           shouldBe BadRequest
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("Unsupported Event Type")
    }

    s"$BadRequest if one of the handlers supports the given payload but it's malformed" in new TestCase {

      (subscriptionsRegistry.handle _)
        .expects(requestContent)
        .returning(EventSchedulingResult.BadRequest.pure[IO])

      val response = processEvent(request).unsafeRunSync()

      response.status                           shouldBe BadRequest
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("Malformed event")
    }

    s"$TooManyRequests if the handler returns ${EventSchedulingResult.Busy}" in new TestCase {

      (subscriptionsRegistry.handle _)
        .expects(requestContent)
        .returning(EventSchedulingResult.Busy.pure[IO])

      val response = processEvent(request).unsafeRunSync()

      response.status                          shouldBe TooManyRequests
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage("Too many events to handle")
    }

    s"$InternalServerError if the handler returns ${EventSchedulingResult.SchedulingError}" in new TestCase {

      (subscriptionsRegistry.handle _)
        .expects(requestContent)
        .returning(EventSchedulingResult.SchedulingError(exceptions.generateOne).pure[IO])

      val response = processEvent(request).unsafeRunSync()

      response.status                           shouldBe InternalServerError
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("Failed to schedule event")
    }

    s"$InternalServerError if the handler fails" in new TestCase {

      (subscriptionsRegistry.handle _)
        .expects(requestContent)
        .returning(exceptions.generateOne.raiseError[IO, EventSchedulingResult])

      val response = processEvent(request).unsafeRunSync()

      response.status                           shouldBe InternalServerError
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("Failed to schedule event")
    }
  }

  private trait TestCase {
    val requestContent = eventRequestContents.generateOne
    private val multipartContent: Multipart[IO] = Multipart[IO](
      Vector(
        Part
          .formData[IO]("event", requestContent.event.noSpaces, `Content-Type`(MediaType.application.json))
          .some,
        requestContent.maybePayload.map(Part.formData[IO]("payload", _))
      ).flatten
    )
    val request = Request(Method.POST, uri"events")
      .withEntity(multipartContent)
      .withHeaders(multipartContent.headers)

    val subscriptionsRegistry = mock[EventConsumersRegistry[IO]]

    val processEvent = new EventEndpointImpl[IO](
      subscriptionsRegistry
    ).processEvent _

  }
}
