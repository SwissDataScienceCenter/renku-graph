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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated

import cats.MonadError
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventBody
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.events.categories.models.Project
import io.circe._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Try}

class EventBodyDeserialiserSpec extends AnyWordSpec with should.Matchers {

  "toJsonLDTriples" should {

    "produce TriplesGeneratedEvent if the Json string can be successfully deserialized" in new TestCase {
      deserializer.toTriplesGeneratedEvent(compoundEventId, triplesGenerationEvent(jsonldTriples)) shouldBe context
        .pure(
          TriplesGeneratedEvent(
            compoundEventId.id,
            Project(projectId, projectPath),
            jsonldTriples,
            schemaVersion
          )
        )

    }

    "fail if parsing fails" in new TestCase {
      val Failure(ParsingFailure(message, underlying)) =
        deserializer.toTriplesGeneratedEvent(compoundEventId, EventBody("{"))

      message    shouldBe "TriplesGeneratedEvent cannot be deserialised: '{'"
      underlying shouldBe a[ParsingFailure]
    }

    "fail if decoding fails" in new TestCase {
      val Failure(DecodingFailure(message, _)) = deserializer.toTriplesGeneratedEvent(compoundEventId, EventBody("{}"))

      message shouldBe "TriplesGeneratedEvent cannot be deserialised: '{}'"
    }
  }

  private trait TestCase {
    val context         = MonadError[Try, Throwable]
    val compoundEventId = compoundEventIds.generateOne

    val jsonldTriples = jsonLDTriples.generateOne
    val schemaVersion = projectSchemaVersions.generateOne

    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val deserializer = new EventBodyDeserializerImpl[Try]

    def triplesGenerationEvent(jsonldTriples: JsonLDTriples): EventBody = EventBody {
      Json
        .obj(
          "id" -> Json.fromString(compoundEventId.id.value),
          "project" -> Json.obj(
            "id"   -> Json.fromInt(projectId.value),
            "path" -> Json.fromString(projectPath.value)
          ),
          "body"          -> Json.fromString(jsonldTriples.value.noSpaces),
          "schemaVersion" -> Json.fromString(schemaVersion.value)
        )
        .noSpaces
    }
  }
}
