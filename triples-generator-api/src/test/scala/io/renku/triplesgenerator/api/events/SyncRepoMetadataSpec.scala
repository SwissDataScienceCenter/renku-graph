/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import Generators.syncRepoMetadataEvents
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.projects
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SyncRepoMetadataSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks with EitherValues {

  "json codec" should {

    "encode and decode the event" in {
      val event = syncRepoMetadataEvents.generateOne
      event.asJson.hcursor.as[SyncRepoMetadata].value shouldBe event
    }

    "be able to decode json valid from the contract point of view" in {
      json"""{
        "categoryName": "SYNC_REPO_METADATA",
        "project": {
          "slug": "project/path"
        }
      }""".hcursor.as[SyncRepoMetadata].value shouldBe SyncRepoMetadata(projects.Slug("project/path"))
    }

    "fail if categoryName does not match" in {

      val otherCategory = nonEmptyStrings().generateOne
      val result = json"""{
        "categoryName": $otherCategory,
        "project": {
          "slug": ${projectSlugs.generateOne}
        }
      }""".hcursor.as[SyncRepoMetadata]

      result.left.value.getMessage() should include(s"Expected SYNC_REPO_METADATA but got $otherCategory")
    }
  }
}
