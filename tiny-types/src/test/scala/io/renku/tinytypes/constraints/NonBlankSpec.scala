/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.tinytypes.constraints

import io.renku.generators.Generators.nonEmptyStrings
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class NonBlankSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "NonBlank" should {

    "be instantiatable when values are not blank" in {
      forAll(nonEmptyStrings()) { someValue =>
        NonBlankString(someValue).toString shouldBe someValue.toString
      }
    }

    "throw an IllegalArgumentException for empty String values" in {
      intercept[IllegalArgumentException](
        NonBlankString("")
      ).getMessage shouldBe "io.renku.tinytypes.constraints.NonBlankString cannot be blank"
    }

    "throw an IllegalArgumentException for blank String values" in {
      intercept[IllegalArgumentException](
        NonBlankString(" ")
      ).getMessage shouldBe "io.renku.tinytypes.constraints.NonBlankString cannot be blank"
    }
  }
}

private class NonBlankString private (val value: String) extends AnyVal with StringTinyType

private object NonBlankString extends TinyTypeFactory[NonBlankString](new NonBlankString(_)) with NonBlank
