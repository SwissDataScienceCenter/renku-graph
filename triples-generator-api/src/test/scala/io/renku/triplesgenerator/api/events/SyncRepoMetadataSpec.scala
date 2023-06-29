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

package io.renku.triplesgenerator.api.events

import Generators.{syncRepoMetadataEvents, syncRepoMetadataWithoutPayloadEvents}
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.renku.compression.Zip
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.generators.jsonld.JsonLDGenerators.jsonLDEntities
import io.renku.graph.model.EventsGenerators.zippedEventPayloads
import io.renku.graph.model.RenkuTinyTypeGenerators.projectPaths
import io.renku.graph.model.events.ZippedEventPayload
import io.renku.graph.model.projects
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{EitherValues, OptionValues}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SyncRepoMetadataSpec
    extends AsyncWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with OptionValues
    with EitherValues
    with AsyncIOSpec
    with AsyncMockFactory {

  "maybeJsonLDPayload" should {

    "return None if no payload given" in {
      syncRepoMetadataWithoutPayloadEvents.generateOne.maybeJsonLDPayload shouldBe None
    }

    "return some failure if payload cannot be unzipped or parsed to JSON-LD" in {

      val result = syncRepoMetadataEvents[IO].map(
        _.generateOne
          .copy(maybePayload = zippedEventPayloads.generateSome)
          .maybeJsonLDPayload
          .value
      )

      result.asserting(_.left.value shouldBe a[Exception])
    }

    "return some JSON-LD payload for a valid payload" in {

      val jsonLDPayload = jsonLDEntities.generateOne

      syncRepoMetadataEvents[IO]
        .map(
          _.generateOne
            .copy(maybePayload = ZippedEventPayload(Zip.zip[IO](jsonLDPayload.toJson.noSpaces).unsafeRunSync()).some)
            .maybeJsonLDPayload
        )
        .asserting(_.value shouldBe jsonLDPayload.asRight)
    }
  }

  "json codec" should {

    "encode and decode the event part" in {
      syncRepoMetadataEvents[IO]
        .map(_.generateOne)
        .asserting(event => event.asJson.hcursor.as[SyncRepoMetadata].value shouldBe event.copy(maybePayload = None))
    }

    "be able to decode json valid from the contract point of view" in {
      json"""{
        "categoryName": "SYNC_REPO_METADATA",
        "project": {
          "path": "project/path"
        }
      }""".hcursor.as[SyncRepoMetadata].value shouldBe SyncRepoMetadata(projects.Path("project/path"),
                                                                        maybePayload = None
      )
    }

    "fail if categoryName does not match" in {

      val otherCategory = nonEmptyStrings().generateOne
      val result = json"""{
        "categoryName": $otherCategory,
        "project": {
          "path": ${projectPaths.generateOne}
        }
      }""".hcursor.as[SyncRepoMetadata]

      result.left.value.getMessage() should include(s"Expected SYNC_REPO_METADATA but got $otherCategory")
    }
  }
}
