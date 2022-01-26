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

import io.renku.tinytypes.TestTinyTypes.FloatTestType
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class FiniteFloatSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "FiniteFloat" should {

    "be instantiatable when values are finite float numbers" in {
      forAll(Gen.choose(Float.MinValue, Float.MaxValue)) { someValue =>
        FloatTestType(someValue).value shouldBe someValue
      }
    }

    Float.PositiveInfinity :: Float.NegativeInfinity :: Float.NaN :: Nil foreach { infiniteValue =>
      s"throw an IllegalArgumentException for $infiniteValue" in {
        intercept[IllegalArgumentException](
          FloatTestType(infiniteValue)
        ).getMessage shouldBe s"io.renku.tinytypes.TestTinyTypes.FloatTestType has to be a finite Float number, not $infiniteValue"
      }
    }
  }
}
