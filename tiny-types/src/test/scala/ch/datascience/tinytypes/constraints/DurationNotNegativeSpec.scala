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

package ch.datascience.tinytypes.constraints

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.tinytypes.{DurationTinyType, TinyTypeFactory}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Duration

class DurationNotNegativeSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "DurationNotNegative" should {

    "be instantiatable when values are positive durations" in {
      forAll(notNegativeJavaDurations) { someValue =>
        DurationNotNegativeType(someValue).value shouldBe someValue
      }
    }

    "throw an IllegalArgumentException for negative duration" in {
      intercept[IllegalArgumentException] {
        DurationNotNegativeType(Duration.ofMillis(negativeInts().generateOne))
      }.getMessage shouldBe "ch.datascience.tinytypes.constraints.DurationNotNegativeType cannot have a negative duration"
    }
  }
}

private class DurationNotNegativeType private (val value: Duration) extends AnyVal with DurationTinyType

private object DurationNotNegativeType
    extends TinyTypeFactory[DurationNotNegativeType](new DurationNotNegativeType(_))
    with DurationNotNegative
