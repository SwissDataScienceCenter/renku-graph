/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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
import ch.datascience.tinytypes.{ TinyType, TinyTypeFactory }
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.prop.PropertyChecks

class NonBlankSpec extends WordSpec with PropertyChecks {

  "NonBlank" should {

    "be instantiatable when values are not blank" in {
      forAll( nonEmptyStrings() ) { someValue =>
        NonBlankString( someValue ).toString shouldBe someValue.toString
      }
    }

    "throw an IllegalArgumentException for empty String values" in {
      intercept[IllegalArgumentException]( NonBlankString( "" ) ).getMessage shouldBe "NonBlankString cannot be blank"
    }

    "throw an IllegalArgumentException for blank String values" in {
      intercept[IllegalArgumentException]( NonBlankString( " " ) ).getMessage shouldBe "NonBlankString cannot be blank"
    }
  }
}

private class NonBlankString private ( val value: String ) extends AnyVal with TinyType[String]

private object NonBlankString
  extends TinyTypeFactory[String, NonBlankString]( new NonBlankString( _ ) )
  with NonBlank
