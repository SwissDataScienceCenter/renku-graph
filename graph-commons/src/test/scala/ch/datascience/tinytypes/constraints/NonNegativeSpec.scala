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

import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class NonNegativeSpec extends WordSpec with ScalaCheckPropertyChecks {

  "NonNegative" should {

    "be instantiatable when values are greater or equal zero" in {
      forAll(Gen.choose(0, 100000)) { someValue =>
        NonNegativeInt(someValue).value shouldBe someValue
      }
    }

    "throw an IllegalArgumentException for negative value" in {
      intercept[IllegalArgumentException](NonNegativeInt(-1)).getMessage shouldBe "ch.datascience.tinytypes.constraints.NonNegativeInt cannot be < 0"
    }
  }
}

private class NonNegativeInt private (val value: Int) extends AnyVal with TinyType[Int]

private object NonNegativeInt extends TinyTypeFactory[Int, NonNegativeInt](new NonNegativeInt(_)) with NonNegative
