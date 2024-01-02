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

package io.renku.knowledgegraph.projects.update

import Generators._
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.blankStrings
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectUpdatesSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks with EitherValues {

  "onlyGLUpdateNeeded" should {

    "return true if at least image and/or visibility is updated but not desc and keywords" in {
      forAll(
        projectUpdatesGen
          .suchThat(u => (u.newImage orElse u.newVisibility).isDefined)
          .map(_.copy(newDescription = None, newKeywords = None))
      )(_.onlyGLUpdateNeeded shouldBe true)
    }

    "return false otherwise" in {
      forAll(
        projectUpdatesGen
          .suchThat(u => (u.newDescription orElse u.newKeywords).isDefined)
      )(_.onlyGLUpdateNeeded shouldBe false)
    }
  }

  "glUpdateNeeded" should {

    "return true if at least image and/or visibility is updated" in {
      forAll(
        projectUpdatesGen
          .suchThat(u => (u.newImage orElse u.newVisibility).isDefined)
      )(_.glUpdateNeeded shouldBe true)
    }

    "return false otherwise" in {
      forAll(projectUpdatesGen.map(_.copy(newImage = None, newVisibility = None)))(
        _.glUpdateNeeded shouldBe false
      )
    }
  }

  "coreUpdateNeeded" should {

    "return true if at least description and/or keywords is updated" in {
      forAll(
        projectUpdatesGen
          .suchThat(u => (u.newDescription orElse u.newKeywords).isDefined)
      )(_.coreUpdateNeeded shouldBe true)
    }

    "return false otherwise" in {
      forAll(projectUpdatesGen.map(_.copy(newDescription = None, newKeywords = None)))(
        _.coreUpdateNeeded shouldBe false
      )
    }
  }

  "JSON encode/decode" should {

    "encode/decode all standard cases" in {
      forAll(projectUpdatesGen.suchThat(_.newImage.isEmpty)) { updates =>
        updates.asJson.hcursor.as[ProjectUpdates].value shouldBe updates
      }
    }

    "lack of the description property to be considered as no-op for the property" in {

      val updates = ProjectUpdates.empty.copy(newDescription = None)

      updates.asJson shouldBe json"""{}"""

      updates.asJson.hcursor.as[ProjectUpdates].value shouldBe updates
    }

    "description = null to be considered as description removal" in {

      val updates = ProjectUpdates.empty.copy(newDescription = Some(None))

      updates.asJson shouldBe json"""{"description":  null}"""

      updates.asJson.hcursor.as[ProjectUpdates].value shouldBe updates
    }

    "description with a blank value to be considered as description removal" in {

      val json = json"""{"description":  ${blankStrings().generateOne}}"""

      json.asJson.hcursor.as[ProjectUpdates].value shouldBe ProjectUpdates.empty.copy(newDescription = Some(None))
    }

    "lack of the image property to be considered as no-op for the property" in {

      val updates = ProjectUpdates.empty.copy(newImage = None)

      updates.asJson shouldBe json"""{}"""

      updates.asJson.hcursor.as[ProjectUpdates].value shouldBe updates
    }
  }
}
