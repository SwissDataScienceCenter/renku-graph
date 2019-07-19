/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.hookcreation

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.tinytypes.TinyType
import ch.datascience.webhookservice.project.ProjectHookUrlFinder.ProjectHookUrl
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class ProjectHookUrlSpec extends WordSpec {

  "ProjectHookUrl" should {

    "be a TinyType" in {
      ProjectHookUrl(validatedUrls.generateOne.value) shouldBe a[TinyType]
    }
  }

  "instantiate" should {

    "be successful for valid urls" in {
      val url = validatedUrls.generateOne

      val Right(projectHookUrl) = ProjectHookUrl.from(url.value)

      projectHookUrl.value shouldBe url.value
    }

    "fail if the value is not a valid url" in {
      val Left(exception) = ProjectHookUrl.from("123")

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "'123' is not a valid ch.datascience.webhookservice.project.ProjectHookUrlFinder.ProjectHookUrl"
    }
  }
}
