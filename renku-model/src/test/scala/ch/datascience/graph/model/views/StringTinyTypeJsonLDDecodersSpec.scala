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

package ch.datascience.graph.model.views

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{blankStrings, nonEmptyStrings}
import ch.datascience.tinytypes.TestTinyTypes.StringTestType
import io.renku.jsonld.JsonLD
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class StringTinyTypeJsonLDDecodersSpec extends AnyWordSpec with should.Matchers {
  import StringTinyTypeJsonLDDecoders._

  "blankStringToNoneDecoder" should {

    "map a non-blank String value to a NonBlank" in {
      val value = nonEmptyStrings().generateOne
      JsonLD.fromString(value).cursor.as[Option[StringTestType]] shouldBe Right(Some(StringTestType(value)))
    }

    "map a blank String value to None" in {
      val value = blankStrings().generateOne
      JsonLD.fromString(value).cursor.as[Option[StringTestType]] shouldBe Right(None)
    }

    "map None to None" in {
      JsonLD.Null.cursor.as[Option[StringTestType]] shouldBe Right(None)
    }

    "fail if the value is non-blank but invalid" in {
      JsonLD.fromString(StringTestType.InvalidValue).cursor.as[Option[StringTestType]] shouldBe a[Left[_, _]]
    }
  }
}
