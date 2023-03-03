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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.triplesgenerated

import cats.syntax.all._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.compression.Zip
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.events.EventRequestContent
import io.renku.generators.Generators.{exceptions, jsons}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.jsonld.JsonLDGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.{CompoundEventId, ZippedEventPayload}
import io.renku.testtools.IOSpec
import org.scalacheck.Arbitrary
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.EitherValues

class EventDecoderSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers with EitherValues {

  "decode" should {

    "produce TriplesGeneratedEvent if the byte array payload can be successfully deserialized to JsonLD" in new TestCase {

      (zip.unzip _)
        .expects(originalPayload.value)
        .returns(jsonld.toJson.noSpaces.asRight)

      decoder
        .decode(EventRequestContent.WithPayload((compoundEventId -> project).asJson, originalPayload))
        .value shouldBe TriplesGeneratedEvent(compoundEventId.id, project, jsonld)
    }

    "fail if unzipping fails" in new TestCase {

      val exception = exceptions.generateOne
      (zip.unzip _)
        .expects(where((arr: Array[Byte]) => arr.sameElements(originalPayload.value)))
        .returns(exception.asLeft)

      decoder
        .decode(EventRequestContent.WithPayload((compoundEventId -> project).asJson, originalPayload))
        .left
        .value
        .getMessage shouldBe exception.getMessage
    }

    "fail if parsing fails" in new TestCase {

      (zip.unzip _)
        .expects(originalPayload.value)
        .returns("{".asRight)

      decoder
        .decode(EventRequestContent.WithPayload((compoundEventId -> project).asJson, originalPayload))
        .left
        .value
        .getMessage shouldBe s"Event $compoundEventId cannot be decoded"
    }

    "fail for event without payload" in new TestCase {
      decoder
        .decode(EventRequestContent.NoPayload(jsons.generateOne))
        .left
        .value
        .getMessage shouldBe "Event without or invalid payload"
    }
  }

  private trait TestCase {

    val originalPayload =
      Arbitrary.arbByte.arbitrary.toGeneratorOfList().map(_.toArray).generateAs(ZippedEventPayload)

    val compoundEventId = compoundEventIds.generateOne
    val project         = consumerProjects.generateOne.copy(id = compoundEventId.projectId)
    val jsonld          = jsonLDValues.generateOne

    val zip     = mock[Zip]
    val decoder = new EventDecoderImpl(zip)
  }

  private implicit lazy val encoder: Encoder[(CompoundEventId, Project)] =
    Encoder.instance { case (eventId, project) =>
      json"""{
      "categoryName": "TRIPLES_GENERATED",
      "id": ${eventId.id},
      "project": {
        "id":   ${project.id},
        "path": ${project.path}
      }
    }"""
    }
}
