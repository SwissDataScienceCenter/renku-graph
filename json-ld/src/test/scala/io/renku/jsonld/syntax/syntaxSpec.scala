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

package io.renku.jsonld.syntax

import io.renku.jsonld._
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.Generators._
import io.renku.jsonld.generators.JsonLDGenerators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class syntaxSpec extends WordSpec with ScalaCheckPropertyChecks {

  "asJsonLD" should {

    "convert a String object into a JsonLD" in {
      forAll { value: String =>
        value.asJsonLD shouldBe JsonLD.fromString(value)
      }
    }

    "convert an Int object into a JsonLD" in {
      forAll { value: Int =>
        value.asJsonLD shouldBe JsonLD.fromInt(value)
      }
    }

    "convert a Long object into a JsonLD" in {
      forAll { value: Long =>
        value.asJsonLD shouldBe JsonLD.fromLong(value)
      }
    }

    "convert a custom value object into a JsonLD" in {
      case class ValueObject(value: String)
      implicit val encoder: JsonLDEncoder[ValueObject] = JsonLDEncoder.instance(o => JsonLD.fromString(o.value))

      forAll { value: String =>
        ValueObject(value).asJsonLD shouldBe JsonLD.fromString(value)
      }
    }

    "convert a custom object into a JsonLD" in {
      val url        = httpUrls.generateOne
      val schema     = schemas.generateOne
      val objectType = schema / nonEmptyStrings().generateOne

      case class Object(string: String, int: Int)
      implicit val encoder: JsonLDEncoder[Object] = JsonLDEncoder.instance { o =>
        JsonLD.entity(
          EntityId.fromAbsoluteUri(s"$url/${o.string}"),
          EntityType.fromProperty(objectType),
          schema / "string" -> o.string.asJsonLD,
          schema / "int"    -> o.int.asJsonLD
        )
      }

      forAll { (string: String, int: Int) =>
        Object(string, int).asJsonLD shouldBe JsonLD.entity(
          EntityId.fromAbsoluteUri(s"$url/$string"),
          EntityType.fromProperty(objectType),
          schema / "string" -> string.asJsonLD,
          schema / "int"    -> int.asJsonLD
        )
      }
    }

    "convert a custom nested objects into a JsonLD" in {
      val url        = httpUrls.generateOne
      val schema     = schemas.generateOne
      val objectType = schema / nonEmptyStrings().generateOne
      val childType  = schema / nonEmptyStrings().generateOne

      case class Object(string:   String, child: ChildObject)
      case class ChildObject(int: Int)
      implicit val childEncoder: JsonLDEncoder[ChildObject] = JsonLDEncoder.instance { o =>
        JsonLD.entity(
          EntityId.fromAbsoluteUri(s"$url/${o.int}"),
          EntityType.fromProperty(childType),
          schema / "int" -> o.int.asJsonLD
        )
      }
      implicit val objectEncoder: JsonLDEncoder[Object] = JsonLDEncoder.instance { o =>
        JsonLD.entity(
          EntityId.fromAbsoluteUri(s"$url/${o.string}"),
          EntityType.fromProperty(objectType),
          schema / "string" -> o.string.asJsonLD,
          schema / "child"  -> o.child.asJsonLD
        )
      }

      forAll { (string: String, int: Int) =>
        Object(string, ChildObject(int)).asJsonLD shouldBe JsonLD.entity(
          EntityId.fromAbsoluteUri(s"$url/$string"),
          EntityType.fromProperty(objectType),
          schema / "string" -> string.asJsonLD,
          schema / "child" -> JsonLD.entity(
            EntityId.fromAbsoluteUri(s"$url/$int"),
            EntityType.fromProperty(childType),
            schema / "int" -> int.asJsonLD
          )
        )
      }
    }

    "convert a custom nested objects defined by @id only into a JsonLD" in {
      val url        = httpUrls.generateOne
      val schema     = schemas.generateOne
      val objectType = schema / nonEmptyStrings().generateOne

      case class Object(string:   String, child: ChildObject)
      case class ChildObject(int: Int)
      implicit val childEncoder: JsonLDEncoder[ChildObject] = JsonLDEncoder.entityId { o =>
        EntityId.fromAbsoluteUri(s"$url/${o.int}")
      }
      implicit val objectEncoder: JsonLDEncoder[Object] = JsonLDEncoder.instance { o =>
        JsonLD.entity(
          EntityId.fromAbsoluteUri(s"$url/${o.string}"),
          EntityType.fromProperty(objectType),
          schema / "string" -> o.string.asJsonLD,
          schema / "child"  -> o.child.asJsonLD
        )
      }

      forAll { (string: String, int: Int) =>
        Object(string, ChildObject(int)).asJsonLD shouldBe JsonLD.entity(
          EntityId.fromAbsoluteUri(s"$url/$string"),
          EntityType.fromProperty(objectType),
          schema / "string" -> string.asJsonLD,
          schema / "child"  -> JsonLD.fromEntityId(EntityId.fromAbsoluteUri(s"$url/$int"))
        )
      }
    }
  }
}
