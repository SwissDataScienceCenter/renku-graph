package io.renku.eventlog.subscriptions

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.server.EndpointTester.jsonEntityDecoder
import io.circe.Json
import org.http4s.MediaType
import org.http4s.headers.`Content-Type`
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventEncoderSpec extends AnyWordSpec with should.Matchers {
  "encodeParts" should {
    "encode only event part for events without payload" in {
      val content = jsons.generateOne
      lazy val encodeEvent: String => Json = _ => content
      val Vector(part) = EventEncoder(encodeEvent).encodeParts[IO](nonEmptyStrings().generateOne)
      part.headers.toList             should contain(`Content-Type`(MediaType.application.json))
      part.name                     shouldBe Some("event")
      part.as[Json].unsafeRunSync() shouldBe content
    }

    "encode event and payload parts" in {
      val eventContent   = jsons.generateOne
      val payloadContent = nonEmptyStrings().generateOne
      val encodeEvent:   String => Json   = _ => eventContent
      val encodePayload: String => String = _ => payloadContent
      val Vector(eventPart, payloadPart) =
        EventEncoder(encodeEvent, encodePayload).encodeParts[IO](nonEmptyStrings().generateOne)
      eventPart.headers.toList             should contain(`Content-Type`(MediaType.application.json))
      eventPart.name                     shouldBe Some("event")
      eventPart.as[Json].unsafeRunSync() shouldBe eventContent

      payloadPart.headers.toList               should contain(`Content-Type`(MediaType.text.plain))
      payloadPart.name                       shouldBe Some("payload")
      payloadPart.as[String].unsafeRunSync() shouldBe payloadContent
    }
  }
}
