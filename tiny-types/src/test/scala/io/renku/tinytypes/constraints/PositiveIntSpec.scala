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

package io.renku.tinytypes.constraints

import io.renku.tinytypes.{IntTinyType, TinyTypeFactory}
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PositiveIntSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "PositiveInt" should {

    "be instantiatable when values are greater than zero" in {
      forAll(Gen.choose(1, 100000)) { someValue =>
        PositiveIntTest(someValue).value shouldBe someValue
      }
    }

    "throw an IllegalArgumentException for 0" in {
      intercept[IllegalArgumentException](
        PositiveIntTest(0)
      ).getMessage shouldBe "io.renku.tinytypes.constraints.PositiveIntTest cannot be <= 0"
    }

    "throw an IllegalArgumentException for negative value" in {
      intercept[IllegalArgumentException](
        PositiveIntTest(-1)
      ).getMessage shouldBe "io.renku.tinytypes.constraints.PositiveIntTest cannot be <= 0"
    }
  }
}

private class PositiveIntTest private (val value: Int) extends AnyVal with IntTinyType
private object PositiveIntTest
    extends TinyTypeFactory[PositiveIntTest](new PositiveIntTest(_))
    with PositiveInt[PositiveIntTest]
