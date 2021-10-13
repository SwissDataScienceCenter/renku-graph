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

package io.renku.eventlog.subscriptions.triplesgenerated

import ch.datascience.generators.Generators.Implicits._
import io.circe.Json
import io.circe.literal._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TriplesGeneratedEventEncoderSpec extends AnyWordSpec with should.Matchers {
  "encoderParts" should {

    "return formData part of TriplesGeneratedEvent to Json and Byte array" in {
      val event = triplesGeneratedEvents.generateOne

      val actualJson = TriplesGeneratedEventEncoder.encodeEvent(event)

      actualJson.hcursor.downField("categoryName").as[String] shouldBe Right("TRIPLES_GENERATED")
      actualJson.hcursor.downField("id").as[String]           shouldBe Right(event.id.id.value)
      actualJson.hcursor.downField("project").as[Json]        shouldBe Right(json"""{
                                                                               "id": ${event.id.projectId.value},
                                                                               "path": ${event.projectPath.value}
                                                                              }""")

    }
  }

  "encodePayload" should {
    "serialize TriplesGeneratedEvent payload to a byte array" in {
      val event = triplesGeneratedEvents.generateOne

      TriplesGeneratedEventEncoder.encodePayload(event) shouldBe event.payload.value
    }
  }
}
