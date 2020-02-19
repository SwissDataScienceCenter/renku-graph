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

package io.renku.jsonld

import io.circe.DecodingFailure
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.JsonLDGenerators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class JsonLDDecoderSpec extends WordSpec with ScalaCheckPropertyChecks {

  "implicit decoders" should {

    "allow to successfully decode String JsonLDValues to String" in {
      forAll { value: String =>
        JsonLD.fromString(value).cursor.as[String] shouldBe Right(value)
      }
    }

    "fail to decode non-String JsonLDValues to String" in {
      forAll { value: Int =>
        val json = JsonLD.fromInt(value)
        json.cursor.as[String] shouldBe Left(DecodingFailure(s"Cannot decode $json to String", Nil))
      }
    }

    "allow to successfully decode any JsonLD to JsonLD" in {
      forAll { json: JsonLD =>
        json.cursor.as[JsonLD] shouldBe Right(json)
      }
    }
  }
}
