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

package io.renku.eventlog.subscriptions

import cats.effect.IO
import io.circe.Json
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.server.EndpointTester.jsonEntityDecoder
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
