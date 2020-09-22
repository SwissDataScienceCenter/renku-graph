/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.tinytypes.constraints

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.tinytypes.{RelativePathTinyType, TinyTypeFactory}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class RelativePathSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "RelativePath" should {

    "be a NonBlank" in {
      new RelativePath {} shouldBe a[NonBlank]
    }

    "be instantiatable when values are not starting and ending with '/'" in {
      forAll(nonEmptyStrings()) { someValue =>
        RelativePathString(someValue).toString shouldBe someValue
      }
    }

    "be instantiatable when values are not starting and ending with '/' but have the '/' sign inside" in {
      forAll(relativePaths()) { someValue =>
        RelativePathString(someValue).toString shouldBe someValue
      }
    }

    "throw an IllegalArgumentException for empty String values" in {
      intercept[IllegalArgumentException](
        RelativePathString("")
      ).getMessage shouldBe "ch.datascience.tinytypes.constraints.RelativePathString cannot be blank"
    }
  }

  "/" should {

    "allow to add next path segment which got url encoded" in {
      val path = (relativePaths() map RelativePathString.apply).generateOne
      (path / "path to smth") shouldBe RelativePathString(s"$path/path+to+smth")
    }

    "use provided converted when adding the next path segment" in {
      val path      = (relativePaths() map RelativePathString.apply).generateOne
      val otherPath = (relativePaths(minSegments = 2) map RelativePathString.apply).generateOne
      (path / otherPath) shouldBe RelativePathString(s"$path/$otherPath")
    }
  }
}

private class RelativePathString private (val value: String) extends AnyVal with RelativePathTinyType
private object RelativePathString
    extends TinyTypeFactory[RelativePathString](new RelativePathString(_))
    with RelativePath
    with RelativePathOps[RelativePathString]
