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

package io.renku.triplesgenerator.events.categories.triplesgenerated

import cats.effect.{BracketThrow, IO, Sync}
import cats.syntax.all._
import ch.datascience.compression.Zip
import ch.datascience.events.consumers.Project
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.ZippedEventPayload
import io.renku.jsonld.generators.JsonLDGenerators._
import io.renku.jsonld.parser.ParsingFailure
import org.scalacheck.Arbitrary
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventBodyDeserialiserSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "toEvent" should {

    "produce TriplesGeneratedEvent if the byte array payload can be successfully deserialized to JsonLD" in new TestCase {

      (zip
        .unzip[IO](_: Array[Byte])(_: BracketThrow[IO], _: Sync[IO]))
        .expects(originalPayload.value, *, *)
        .returns(jsonld.toJson.noSpaces.pure[IO])

      deserializer.toEvent(compoundEventId, project, originalPayload).unsafeRunSync() shouldBe TriplesGeneratedEvent(
        compoundEventId.id,
        project,
        jsonld
      )
    }

    "fail if unzipping fails" in new TestCase {
      val exception = exceptions.generateOne
      (zip
        .unzip[IO](_: Array[Byte])(_: BracketThrow[IO], _: Sync[IO]))
        .expects(where((arr: Array[Byte], _, _) => arr.sameElements(originalPayload.value)))
        .returns(exception.raiseError[IO, String])

      intercept[Exception] {
        deserializer.toEvent(compoundEventId, project, originalPayload).unsafeRunSync()
      }.getMessage shouldBe exception.getMessage
    }

    "fail if parsing fails" in new TestCase {
      (zip
        .unzip[IO](_: Array[Byte])(_: BracketThrow[IO], _: Sync[IO]))
        .expects(originalPayload.value, *, *)
        .returns("{".pure[IO])

      intercept[ParsingFailure] {
        deserializer.toEvent(compoundEventId, project, originalPayload).unsafeRunSync()
      }.getMessage shouldBe s"TriplesGeneratedEvent cannot be deserialised: $compoundEventId"
    }
  }

  private trait TestCase {
    val compoundEventId = compoundEventIds.generateOne

    val originalPayload =
      Arbitrary.arbByte.arbitrary.toGeneratorOfList().map(_.toArray).generateAs(ZippedEventPayload)

    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne
    val project     = Project(projectId, projectPath)

    val jsonld = jsonLDValues.generateOne

    val zip = mock[Zip]

    val deserializer = new EventBodyDeserializerImpl[IO](zip)

  }
}
