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

import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.JsonLDGenerators._
import org.scalatest.matchers._
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import syntax._

class JsonLDEncoderSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "instance" should {

    "allow to create a JsonLDEncoder for the given type" in {
      forAll { (id: EntityId, types: EntityTypes, property: Property, value: String) =>
        val encoder = JsonLDEncoder.instance[Object] { o =>
          JsonLD.entity(id, types, property -> o.field.asJsonLD)
        }

        encoder(Object(value)) shouldBe JsonLD.entity(id, types, property -> value.asJsonLD)
      }
    }
  }

  "entityId" should {

    "allow to create a JsonLDEncoder producing EntityId for the given type" in {
      forAll { (value: String) =>
        val encoder = JsonLDEncoder.entityId[Object] { o =>
          EntityId.of(o.field)
        }

        encoder(Object(value)) shouldBe JsonLD.fromEntityId(EntityId.of(value))
      }
    }
  }

  private case class Object(field: String)
}
