package io.renku.commiteventservice.events

import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.renku.events.EventRequestContent
import io.renku.events.Generators.eventRequestContents
import io.renku.events.consumers.{EventConsumersRegistry, EventSchedulingResult}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, jsons, nonEmptyStrings}
import io.renku.http.ErrorMessage.ErrorMessage
import io.renku.http.InfoMessage._
import io.renku.http.client.RestClient.PartEncoder
import io.renku.http.server.EndpointTester._
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.testtools.IOSpec
import io.renku.tinytypes.ByteArrayTinyType
import io.renku.tinytypes.contenttypes.ZippedContent
import org.http4s.MediaType._
import org.http4s.Status.{Accepted, BadRequest, InternalServerError, ServiceUnavailable, TooManyRequests}
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.http4s.multipart.Part
import org.http4s.{Method, Request}
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

    s"return $BadRequest if the request is not a multipart request" in new TestCase {

      val response = endpoint.processEvent(Request()).unsafeRunSync()

      response.status                          shouldBe BadRequest
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage("Not multipart request")
    }

    s"return $BadRequest if there is no event part in the request" in new TestCase {

      override lazy val request =
        Request[IO]().addParts(Part.formData[IO](nonEmptyStrings().generateOne, nonEmptyStrings().generateOne))

      val response = (request >>= endpoint.processEvent).unsafeRunSync()

      response.status                          shouldBe BadRequest
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage("Missing event part")
    }

    s"return $BadRequest if the event part in the request is malformed" in new TestCase {

      override lazy val request = Request[IO]().addParts(Part.formData[IO]("event", ""))

      val response = (request >>= endpoint.processEvent).unsafeRunSync()

      response.status                          shouldBe BadRequest
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage("Malformed event body")
    }

    forAll(
      Table(
        "Request content name" -> "Request content",
        "no payload"           -> jsons.map(EventRequestContent.NoPayload),
        "string payload" -> (jsons -> nonEmptyStrings()).mapN { case (event, payload) =>
          EventRequestContent.WithPayload(event, payload)
        }
      )
    ) { (scenarioName, requestContents) =>
      s"return $Accepted if one of handlers accepts the given $scenarioName request" in new TestCase {
        override val requestContent: EventRequestContent = requestContents.generateOne

        (subscriptionsRegistry.handle _)
          .expects(where(eventRequestEquals(requestContent)))
          .returning(EventSchedulingResult.Accepted.pure[IO])

        val response = (request >>= endpoint.processEvent).unsafeRunSync()

        response.status                          shouldBe Accepted
        response.contentType                     shouldBe Some(`Content-Type`(application.json))
        response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event accepted")
      }
    }

    s"return $BadRequest if none of handlers supports the given payload" in new TestCase {

      (subscriptionsRegistry.handle _)
        .expects(*)
        .returning(EventSchedulingResult.UnsupportedEventType.pure[IO])

      val response = (request >>= endpoint.processEvent).unsafeRunSync()

      response.status                           shouldBe BadRequest
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("Unsupported Event Type")
    }

    s"return $BadRequest if one of handlers supports the given payload but it's malformed" in new TestCase {

      (subscriptionsRegistry.handle _)
        .expects(*)
        .returning(EventSchedulingResult.BadRequest.pure[IO])

      val response = (request >>= endpoint.processEvent).unsafeRunSync()

      response.status                           shouldBe BadRequest
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("Malformed event")
    }

    s"return $TooManyRequests if handler returns ${EventSchedulingResult.Busy}" in new TestCase {

      (subscriptionsRegistry.handle _)
        .expects(*)
        .returning(EventSchedulingResult.Busy.pure[IO])

      val response = (request >>= endpoint.processEvent).unsafeRunSync()

      response.status                          shouldBe TooManyRequests
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe ErrorMessage("Too many events to handle")
    }

    s"return $InternalServerError if handler returns ${EventSchedulingResult.SchedulingError}" in new TestCase {

      (subscriptionsRegistry.handle _)
        .expects(*)
        .returning(EventSchedulingResult.SchedulingError(exceptions.generateOne).pure[IO])

      val response = (request >>= endpoint.processEvent).unsafeRunSync()

      response.status                           shouldBe InternalServerError
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("Failed to schedule event")
    }

    s"return $ServiceUnavailable if handler returns EventSchedulingResult.ServiceUnavailable" in new TestCase {

      val handlingResult = EventSchedulingResult.ServiceUnavailable(nonEmptyStrings().generateOne)
      (subscriptionsRegistry.handle _)
        .expects(*)
        .returning(handlingResult.pure[IO])

      val response = (request >>= endpoint.processEvent).unsafeRunSync()

      response.status                          shouldBe ServiceUnavailable
      response.contentType                     shouldBe Some(`Content-Type`(application.json))
      response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage(handlingResult.reason)
    }

    s"return $InternalServerError if some handler fails" in new TestCase {

      (subscriptionsRegistry.handle _)
        .expects(*)
        .returning(exceptions.generateOne.raiseError[IO, EventSchedulingResult])

      val response = (request >>= endpoint.processEvent).unsafeRunSync()

      response.status                           shouldBe InternalServerError
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("Failed to schedule event")
    }
  }

  private trait TestCase {
    val requestContent = eventRequestContents.generateOne

    private lazy val multipartParts: Vector[Part[IO]] = requestContent match {
      case EventRequestContent.NoPayload(event) =>
        Vector(implicitly[PartEncoder[Json]].encode("event", event))
      case EventRequestContent.WithPayload(event, payload: String) =>
        Vector(
          implicitly[PartEncoder[Json]].encode("event", event),
          implicitly[PartEncoder[String]].encode("payload", payload)
        )
      case EventRequestContent.WithPayload(event, payload: ByteArrayTinyType with ZippedContent) =>
        Vector(
          implicitly[PartEncoder[Json]].encode("event", event),
          implicitly[PartEncoder[ByteArrayTinyType with ZippedContent]].encode("payload", payload)
        )
      case event: EventRequestContent.WithPayload[_] => fail(s"Unsupported EventRequestContent payload type $event")
    }

    lazy val request = Request[IO](Method.POST, uri"events").addParts(multipartParts: _*)

    val subscriptionsRegistry = mock[EventConsumersRegistry[IO]]
    val endpoint              = new EventEndpointImpl[IO](subscriptionsRegistry)
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
