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

package io.renku.knowledgegraph.projects.update

import Generators._
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.blankStrings
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectUpdatesSpec extends AnyFlatSpec with should.Matchers with ScalaCheckPropertyChecks with EitherValues {

  it should "encode/decode " in {
    forAll(projectUpdatesGen) { updates =>
      updates.asJson.hcursor.as[ProjectUpdates].value shouldBe updates
    }
  }

  it should "lack of the image property to be considered as no-op for the property" in {

    val updates = ProjectUpdates.empty.copy(newImage = None)

    updates.asJson shouldBe json"""{}"""

    updates.asJson.hcursor.as[ProjectUpdates].value shouldBe updates
  }

  it should "image = null to be considered as descriptions removals" in {

    val updates = ProjectUpdates.empty.copy(newImage = Some(None))

    updates.asJson shouldBe json"""{"image":  null}"""

    updates.asJson.hcursor.as[ProjectUpdates].value shouldBe updates
  }

  it should "image with a blank value to be considered as description removals" in {

    val json = json"""{"image":  ${blankStrings().generateOne}}"""

    json.asJson.hcursor.as[ProjectUpdates].value shouldBe ProjectUpdates.empty.copy(newImage = Some(None))
  }
}
