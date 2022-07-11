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

import io.renku.generators.Generators.{noDashUuid, nonEmptyStrings}
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}
import org.scalacheck.Gen.uuid
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class UUIDSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "UUID" should {

    "be a NonBlank" in {
      UUIDString shouldBe a[UUID[_]]
      UUIDString shouldBe a[NonBlank[_]]
    }

    "be instantiatable when values are valid UUIDs" in {
      forAll(uuid) { expected =>
        val Right(UUIDString(actual)) = UUIDString.from(expected.toString)
        actual shouldBe expected.toString
      }
    }

    "be instantiatable when values are UUIDs without dashes" in {
      forAll(noDashUuid) { expected =>
        val Right(UUIDString(actual)) = UUIDString.from(expected)
        actual shouldBe expected
      }
    }

    "fail instantiation for non-UUID values" in {
      forAll(nonEmptyStrings()) { value =>
        intercept[IllegalArgumentException] {
          UUIDString(value)
        }.getMessage shouldBe s"'$value' is not a valid UUID value for io.renku.tinytypes.constraints.UUIDString"
      }
    }
  }
}

private class UUIDString private (val value: String) extends AnyVal with StringTinyType
private object UUIDString extends TinyTypeFactory[UUIDString](new UUIDString(_)) with UUID[UUIDString]
