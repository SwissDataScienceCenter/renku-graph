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

package io.renku.triplesgenerator.events

import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.renku.events.EventRequestContent
import io.renku.events.Generators._
import io.renku.events.consumers._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators.zippedEventPayloads
import io.renku.http.ErrorMessage.ErrorMessage
import io.renku.http.InfoMessage._
import io.renku.http.client.RestClient._
import io.renku.http.server.EndpointTester._
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.testtools.IOSpec
import io.renku.tinytypes.ByteArrayTinyType
import io.renku.tinytypes.contenttypes.ZippedContent
import io.renku.triplesgenerator.reprovisioning.ReProvisioningStatus
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.http4s.multipart.{Multipart, Part}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class EventEndpointSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with should.Matchers
    with TableDrivenPropertyChecks {

  "processEvent" should {

    s"$BadRequest if the request is not a multipart request" in new TestCase {

      givenReProvisioningStatusSet(false)

      val response = endpoint.processEvent(Request()).unsafeRunSync()

      response.status                          shouldBe BadRequest
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage("Not multipart request")
    }

    s"$BadRequest if there is no event part in the request" in new TestCase {

      givenReProvisioningStatusSet(false)

      val multipart =
        Multipart[IO](Vector(Part.formData[IO](nonEmptyStrings().generateOne, nonEmptyStrings().generateOne)))

      val response =
        endpoint.processEvent(Request().withEntity(multipart).withHeaders(multipart.headers)).unsafeRunSync()

      response.status                          shouldBe BadRequest
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage("Missing event part")
    }

    s"$BadRequest if the event part in the request is malformed" in new TestCase {

      givenReProvisioningStatusSet(false)

      val multipart = Multipart[IO](Vector(Part.formData[IO]("event", "")))

      val response =
        endpoint.processEvent(Request().withEntity(multipart).withHeaders(multipart.headers)).unsafeRunSync()

      response.status                          shouldBe BadRequest
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage("Malformed event body")
    }

    val scenarios = Table(
      "Request content name" -> "Request content",
      "no payload"           -> jsons.map(EventRequestContent.NoPayload),
      "string payload" -> (jsons -> nonEmptyStrings()).mapN { case (event, payload) =>
        EventRequestContent.WithPayload(event, payload)
      },
      "zipped payload" -> (jsons -> zippedEventPayloads).mapN { case (event, payload) =>
        EventRequestContent.WithPayload(event, payload)
      }
    )
    forAll(scenarios) { (scenarioName, requestContents) =>
      s"$Accepted if one of the handlers accepts the given $scenarioName request" in new TestCase {
        override val requestContent: EventRequestContent = requestContents.generateOne

        givenReProvisioningStatusSet(false)

        (subscriptionsRegistry.handle _)
          .expects(where(eventRequestEquals(requestContent)))
          .returning(EventSchedulingResult.Accepted.pure[IO])

        val response = endpoint.processEvent(request).unsafeRunSync()

        println(response.as[String].unsafeRunSync())
        response.status                          shouldBe Accepted
        response.contentType                     shouldBe Some(`Content-Type`(application.json))
        response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event accepted")
      }
    }

    s"$BadRequest if none of the handlers supports the given payload" in new TestCase {
      givenReProvisioningStatusSet(false)

      (subscriptionsRegistry.handle _)
        .expects(*)
        .returning(EventSchedulingResult.UnsupportedEventType.pure[IO])

      val response = endpoint.processEvent(request).unsafeRunSync()

      response.status                           shouldBe BadRequest
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("Unsupported Event Type")
    }

    s"$BadRequest if one of the handlers supports the given payload but it's malformed" in new TestCase {
      givenReProvisioningStatusSet(false)

      (subscriptionsRegistry.handle _)
        .expects(*)
        .returning(EventSchedulingResult.BadRequest.pure[IO])

      val response = endpoint.processEvent(request).unsafeRunSync()

      response.status                           shouldBe BadRequest
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("Malformed event")
    }

    s"$TooManyRequests if the handler returns ${EventSchedulingResult.Busy}" in new TestCase {
      givenReProvisioningStatusSet(false)

      (subscriptionsRegistry.handle _)
        .expects(*)
        .returning(EventSchedulingResult.Busy.pure[IO])

      val response = endpoint.processEvent(request).unsafeRunSync()

      response.status                          shouldBe TooManyRequests
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage("Too many events to handle")
    }

    s"$InternalServerError if the handler returns ${EventSchedulingResult.SchedulingError}" in new TestCase {
      givenReProvisioningStatusSet(false)

      (subscriptionsRegistry.handle _)
        .expects(*)
        .returning(EventSchedulingResult.SchedulingError(exceptions.generateOne).pure[IO])

      val response = endpoint.processEvent(request).unsafeRunSync()

      response.status                           shouldBe InternalServerError
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("Failed to schedule event")
    }

    s"$InternalServerError if the handler fails" in new TestCase {

      givenReProvisioningStatusSet(false)

      (subscriptionsRegistry.handle _)
        .expects(*)
        .returning(exceptions.generateOne.raiseError[IO, EventSchedulingResult])

      val response = endpoint.processEvent(request).unsafeRunSync()

      response.status                           shouldBe InternalServerError
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("Failed to schedule event")
    }

    s"return $ServiceUnavailable if re-provisioning flag set to true" in new TestCase {

      givenReProvisioningStatusSet(true)

      val response = endpoint.processEvent(request).unsafeRunSync()

      response.status      shouldBe ServiceUnavailable
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage(
        "Temporarily unavailable: currently re-provisioning"
      )
    }
  }

  private trait TestCase {
    val requestContent = eventRequestContents.generateOne

    private lazy val multipartContent: Multipart[IO] = requestContent match {
      case EventRequestContent.NoPayload(event) =>
        Multipart[IO](
          Vector(
            implicitly[PartEncoder[Json]].encode("event", event)
          )
        )
      case EventRequestContent.WithPayload(event, payload: String) =>
        Multipart[IO](
          Vector(
            implicitly[PartEncoder[Json]].encode("event", event),
            implicitly[PartEncoder[String]].encode("payload", payload)
          )
        )
      case EventRequestContent.WithPayload(event, payload: ByteArrayTinyType with ZippedContent) =>
        Multipart[IO](
          Vector(
            implicitly[PartEncoder[Json]].encode("event", event),
            implicitly[PartEncoder[ByteArrayTinyType with ZippedContent]].encode("payload", payload)
          )
        )
      case event: EventRequestContent.WithPayload[_] => fail(s"Unsupported EventRequestContent payload type $event")
    }

    lazy val request = Request(Method.POST, uri"events")
      .withEntity(multipartContent)
      .withHeaders(multipartContent.headers)

    val subscriptionsRegistry = mock[EventConsumersRegistry[IO]]

    val reProvisioningStatus = mock[ReProvisioningStatus[IO]]
    val endpoint             = new EventEndpointImpl[IO](subscriptionsRegistry, reProvisioningStatus)

    def givenReProvisioningStatusSet(flag: Boolean) =
      (reProvisioningStatus.isReProvisioning _)
        .expects()
        .returning(flag.pure[IO])
  }

  private def eventRequestEquals(eventRequestContent: EventRequestContent): EventRequestContent => Boolean = {
    case requestContent @ EventRequestContent.NoPayload(_)              => requestContent == eventRequestContent
    case requestContent @ EventRequestContent.WithPayload(_, _: String) => requestContent == eventRequestContent
    case EventRequestContent.WithPayload(event, actualPayload: ByteArrayTinyType) =>
      eventRequestContent match {
        case EventRequestContent.WithPayload(_, expectedPayload: ByteArrayTinyType) =>
          eventRequestContent.event == event && (actualPayload.value sameElements expectedPayload.value)
        case _ => false
      }
    case _ => false
  }
}
