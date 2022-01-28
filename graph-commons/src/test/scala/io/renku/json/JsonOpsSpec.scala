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

package io.renku.json

import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class JsonOpsSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  import JsonOps._

  "addIfDefined extension method" should {

    "be adding the property to the JSON being called on if the given value is defined" in {
      forAll(nonBlankStrings(), nonBlankStrings()) { (property, value) =>
        val newJson = existingJson addIfDefined (property.value -> Some(value.toString()))

        newJson shouldBe Json.obj(
          "existing"     -> "value".asJson,
          property.value -> value.toString().asJson
        )
      }
    }

    "not be adding the property to the JSON being called on if the given value is empty" in {

      val newJson = existingJson addIfDefined (nonBlankStrings().generateOne.value -> Option.empty[String])

      newJson shouldBe existingJson
    }
  }

  private val existingJson = json"""{
    "existing": "value"
  }"""
}
