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

package ch.datascience.tinytypes.constraints

import ch.datascience.generators.Generators._
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class RelativePathSpec extends WordSpec with ScalaCheckPropertyChecks {

  "RelativePath" should {

    "be a NonBlank" in {
      new RelativePath {} shouldBe a[NonBlank]
    }

    "be instantiatable when values are not starting and ending with '/'" in {
      forAll(nonEmptyStrings()) { someValue =>
        RelativePathString(someValue).toString shouldBe someValue.toString
      }
    }

    "be instantiatable when values are not starting and ending with '/' but have the '/' sign inside" in {
      forAll(relativePaths()) { someValue =>
        RelativePathString(someValue).toString shouldBe someValue.toString
      }
    }

    "throw an IllegalArgumentException for empty String values" in {
      intercept[IllegalArgumentException](RelativePathString("")).getMessage shouldBe "ch.datascience.tinytypes.constraints.RelativePathString cannot be blank"
    }
  }
}

private class RelativePathString private (val value: String) extends AnyVal with TinyType[String]
private object RelativePathString
    extends TinyTypeFactory[String, RelativePathString](new RelativePathString(_))
    with RelativePath
