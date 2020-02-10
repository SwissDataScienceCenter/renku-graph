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

import io.renku.jsonld.generators.Generators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EntityIdSpec extends WordSpec with ScalaCheckPropertyChecks {

  "of" should {
    "return an EntityId with the given URI in String" in {
      forAll(httpUrls()) { uri =>
        EntityId.of(uri).value shouldBe uri
      }
    }
  }

  "of" should {
    "return an EntityId with the URI created from the given value using the converter" in {
      case class Value(v: String)
      implicit val valueToString: Value => EntityId = v => EntityId.of(v.v)

      forAll(httpUrls() map Value.apply) { value =>
        val entityId = EntityId.of(value)

        entityId       shouldBe a[EntityId]
        entityId.value shouldBe value.v
      }
    }
  }
}
