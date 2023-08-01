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

import io.renku.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import Generators._
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.model.projects
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import org.scalatest.EitherValues

class ProjectViewingDeletionSpec extends AnyWordSpec with should.Matchers with EitherValues {

  "json codec" should {

    "encode and decode" in {

      val event = projectViewingDeletions.generateOne

      event.asJson.hcursor.as[ProjectViewingDeletion].value shouldBe event
    }

    "be able to decode json valid from the contract point of view" in {
      json"""{
        "categoryName": "PROJECT_VIEWING_DELETION",
        "project": {
          "slug": "project/path"
        }
      }""".hcursor.as[ProjectViewingDeletion].value shouldBe
        ProjectViewingDeletion(projects.Slug("project/path"))
    }

    "fail if categoryName does not match" in {

      val otherCategory = nonEmptyStrings().generateOne
      val result = json"""{
        "categoryName": $otherCategory,
        "project": {
          "slug": ${projectSlugs.generateOne}
        }
      }""".hcursor.as[ProjectViewingDeletion]

      result.left.value.getMessage() should include(s"Expected PROJECT_VIEWING_DELETION but got $otherCategory")
    }
  }
}
