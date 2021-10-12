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

package ch.datascience.graph.model

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.entityModel.Location
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class entityModelSpec extends AnyWordSpec with should.Matchers {

  "equals" should {

    "return true if values are the same irrespectively of the Location type" in {
      val locationValue = relativePaths().generateOne

      val file   = Location.File(locationValue)
      val folder = Location.Folder(locationValue)
      file            shouldBe folder
      file.hashCode() shouldBe folder.hashCode()

      val fileOrFolder = Location.FileOrFolder(locationValue)
      file            shouldBe fileOrFolder
      file.hashCode() shouldBe fileOrFolder.hashCode()
    }

    "return false if values are not the same" in {
      val locationValue1 = relativePaths().generateOne
      val locationValue2 = relativePaths().generateOne

      val file1 = Location.File(locationValue1)
      val file2 = Location.File(locationValue2)
      file1            should not be file2
      file1.hashCode() should not be file2.hashCode()

      val folder = Location.Folder(locationValue1)
      file1            shouldBe folder
      file1.hashCode() shouldBe folder.hashCode()

      val fileOrFolder = Location.FileOrFolder(locationValue1)
      file1            shouldBe fileOrFolder
      file1.hashCode() shouldBe fileOrFolder.hashCode()
    }
  }
}
