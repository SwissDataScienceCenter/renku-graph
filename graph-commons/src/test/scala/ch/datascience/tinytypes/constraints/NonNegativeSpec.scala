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

import ch.datascience.tinytypes.{IntTinyType, LongTinyType, TinyTypeFactory}
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class NonNegativeSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "NonNegativeInt" should {

    "be instantiatable when values are greater or equal zero" in {
      forAll(Gen.choose(0, 100000)) { someValue =>
        TestNonNegativeInt(someValue).value shouldBe someValue
      }
    }

    "throw an IllegalArgumentException for negative value" in {
      intercept[IllegalArgumentException](
        TestNonNegativeInt(-1)
      ).getMessage shouldBe "ch.datascience.tinytypes.constraints.TestNonNegativeInt cannot be < 0"
    }
  }

  "NonNegativeLong" should {

    "be instantiatable when values are greater or equal zero" in {
      forAll(Gen.choose(0, 100000)) { someValue =>
        TestNonNegativeLong(someValue).value shouldBe someValue
      }
    }

    "throw an IllegalArgumentException for negative value" in {
      intercept[IllegalArgumentException](
        TestNonNegativeLong(-1)
      ).getMessage shouldBe "ch.datascience.tinytypes.constraints.TestNonNegativeLong cannot be < 0"
    }
  }
}

private class TestNonNegativeInt private (val value: Int) extends AnyVal with IntTinyType
private object TestNonNegativeInt
    extends TinyTypeFactory[TestNonNegativeInt](new TestNonNegativeInt(_))
    with NonNegativeInt

private class TestNonNegativeLong private (val value: Long) extends AnyVal with LongTinyType
private object TestNonNegativeLong
    extends TinyTypeFactory[TestNonNegativeLong](new TestNonNegativeLong(_))
    with NonNegativeLong
