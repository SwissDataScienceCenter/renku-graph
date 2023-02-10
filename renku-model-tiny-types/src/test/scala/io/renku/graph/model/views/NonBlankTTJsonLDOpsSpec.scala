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

package io.renku.graph.model.views

import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{blankStrings, nonEmptyStrings}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import cats.syntax.all._
import io.renku.tinytypes.constraints.NonBlank
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class NonBlankTTJsonLDOpsSpec extends AnyWordSpec with should.Matchers with EitherValues {

  "decode" should {

    "turn a valid string value from Json to the relevant type" in {
      val value = nonEmptyStrings().generateOne
      JsonLD.fromString(value).cursor.as[Option[TestStringTinyType]].value shouldBe Some(TestStringTinyType(value))
    }

    "return a None for a blank value in Json" in {
      val value = blankStrings().generateOne
      JsonLD.fromString(value).cursor.as[Option[TestStringTinyType]].value shouldBe None
    }

    "return a None for a Null Json" in {
      JsonLD.Null.cursor.as[Option[TestStringTinyType]].value shouldBe None
    }
  }

  "encode" should {

    "produce a Json String" in {
      val tt = nonEmptyStrings().generateAs(TestStringTinyType)
      tt.asJsonLD shouldBe JsonLD.fromString(tt.value)
    }
  }

  "failIfNone" should {

    "return the value for Some" in {
      val tinyType = nonEmptyStrings().generateAs(TestStringTinyType)

      TestStringTinyType.failIfNone(tinyType.some) shouldBe tinyType.asRight
    }

    "fail for None" in {
      TestStringTinyType
        .failIfNone(None)
        .left
        .value
        .getMessage() should contain(s"A value of '${TestStringTinyType.typeName}' expected but got none")
    }
  }
}

private final class TestStringTinyType private (val value: String) extends AnyVal with StringTinyType
private object TestStringTinyType
    extends TinyTypeFactory[TestStringTinyType](new TestStringTinyType(_))
    with NonBlank[TestStringTinyType]
    with NonBlankTTJsonLDOps[TestStringTinyType]
