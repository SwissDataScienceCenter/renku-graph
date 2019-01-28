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

package ch.datascience.tinytypes

import org.scalatest.Matchers._
import org.scalatest.WordSpec

class TinyTypeSpec extends WordSpec {

  "toString" should {

    "return a String value of the 'value' property" in {
      ("abc" +: 2 +: 2L +: true +: Nil) foreach { someValue =>
        val tinyType: TinyType[Any] = new TinyType[Any] {
          override val value: Any = someValue
        }

        tinyType.toString shouldBe someValue.toString
      }
    }
  }
}

class TinyTypeFactorySpec extends WordSpec {

  "apply" should {

    "instantiate object of the relevant type" in {
      TinyTypeTest("def").value shouldBe "def"
    }

    "throw an IllegalArgument exception if the first type constraint is not met" in {
      intercept[IllegalArgumentException] {
        TinyTypeTest("abc")
      }.getMessage shouldBe "ch.datascience.tinytypes.TinyTypeTest cannot have 'abc' value"
    }

    "throw an IllegalArgument exception if one of defined type constraints is not met" in {
      intercept[IllegalArgumentException] {
        TinyTypeTest("def!")
      }.getMessage shouldBe "! is not allowed"
    }
  }

  "from" should {

    "return Right with instantiated object for valid values" in {
      TinyTypeTest.from("def").map(_.value) shouldBe Right("def")
    }

    "return Left with the IllegalArgumentException containing errors if the type constraints are not met" in {
      val result = TinyTypeTest.from("abc")

      result shouldBe a[Left[_, _]]

      val Left(exception) = result
      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "ch.datascience.tinytypes.TinyTypeTest cannot have 'abc' value"
    }
  }
}

private class TinyTypeTest private (val value: String) extends AnyVal with TinyType[String]

private object TinyTypeTest extends TinyTypeFactory[String, TinyTypeTest](new TinyTypeTest(_)) {

  addConstraint(
    check   = !_.contains("abc"),
    message = (value: String) => s"$typeName cannot have '$value' value"
  )

  addConstraint(
    check   = !_.contains("!"),
    message = (value: String) => "! is not allowed"
  )
}
