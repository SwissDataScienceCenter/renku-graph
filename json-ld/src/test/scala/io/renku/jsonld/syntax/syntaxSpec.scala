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

import java.time.{Instant, LocalDate}

import io.renku.jsonld._
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.Generators._
import io.renku.jsonld.generators.JsonLDGenerators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class syntaxSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

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

    "convert a Instant into a JsonLD" in {
      forAll { value: Instant =>
        value.asJsonLD shouldBe JsonLD.fromInstant(value)
      }
    }

    "convert a LocalDate into a JsonLD" in {
      forAll { value: LocalDate =>
        value.asJsonLD shouldBe JsonLD.fromLocalDate(value)
      }
    }

    "convert a custom value object into a JsonLD" in {
      case class ValueObject(value: String)
      implicit val encoder: JsonLDEncoder[ValueObject] = JsonLDEncoder.instance(o => JsonLD.fromString(o.value))

      forAll { value: String =>
        ValueObject(value).asJsonLD shouldBe JsonLD.fromString(value)
      }
    }

    "convert an Option into a JsonLD" in {
      forAll { maybeValue: Option[Long] =>
        maybeValue.asJsonLD shouldBe JsonLD.fromOption(maybeValue)
      }
    }

    "convert a sequence of objects into a JsonLD" in {
      forAll { seq: Seq[Long] =>
        seq.asJsonLD shouldBe JsonLD.arr(seq map JsonLD.fromLong: _*)
      }
    }

    "convert a list of objects into a JsonLD" in {
      forAll { seq: List[Long] =>
        seq.asJsonLD shouldBe JsonLD.arr(seq map JsonLD.fromLong: _*)
      }
    }

    "convert a set of objects into a JsonLD" in {
      forAll { set: Set[Long] =>
        set.asJsonLD shouldBe JsonLD.arr(set.toList.sorted map JsonLD.fromLong: _*)
      }
    }

    "convert a custom object into a JsonLD" in {
      forAll { (schema: Schema, id: EntityId, types: EntityTypes, string: String, int: Int) =>
        implicit val encoder: JsonLDEncoder[Object] = JsonLDEncoder.instance { o =>
          JsonLD.entity(
            id,
            types,
            schema / "string" -> o.string.asJsonLD,
            schema / "int"    -> o.int.asJsonLD
          )
        }

        Object(string, int).asJsonLD shouldBe JsonLD.entity(
          id,
          types,
          schema / "string" -> string.asJsonLD,
          schema / "int"    -> int.asJsonLD
        )
      }
    }

    "convert a custom nested objects into a JsonLD" in {

      val schema = schemas.generateOne

      forAll {
        (parentId:    EntityId,
         parentTypes: EntityTypes,
         parentField: String,
         childId:     EntityId,
         childTypes:  EntityTypes,
         childField:  Int
        ) =>
          implicit val childEncoder: JsonLDEncoder[Child] = JsonLDEncoder.instance { o =>
            JsonLD.entity(childId, childTypes, schema / "int" -> o.int.asJsonLD)
          }
          implicit val parentEncoder: JsonLDEncoder[Parent] = JsonLDEncoder.instance { o =>
            JsonLD.entity(parentId,
                          parentTypes,
                          schema / "string" -> o.string.asJsonLD,
                          schema / "child"  -> o.child.asJsonLD
            )
          }

          Parent(parentField, Child(childField)).asJsonLD shouldBe JsonLD.entity(
            parentId,
            parentTypes,
            schema / "string" -> parentField.asJsonLD,
            schema / "child" -> JsonLD.entity(
              childId,
              childTypes,
              schema / "int" -> childField.asJsonLD
            )
          )
      }
    }

    "convert a custom nested objects defined by @id only into a JsonLD" in {
      forAll { (schema: Schema, parentId: EntityId, parentTypes: EntityTypes, parentField: String, childField: Int) =>
        implicit val childEncoder: JsonLDEncoder[Child] = JsonLDEncoder.entityId { o =>
          EntityId.of(schema / o.int)
        }
        implicit val objectEncoder: JsonLDEncoder[Parent] = JsonLDEncoder.instance { o =>
          JsonLD.entity(
            parentId,
            parentTypes,
            schema / "string" -> o.string.asJsonLD,
            schema / "child"  -> o.child.asJsonLD
          )
        }

        Parent(parentField, Child(childField)).asJsonLD shouldBe JsonLD.entity(
          parentId,
          parentTypes,
          schema / "string" -> parentField.asJsonLD,
          schema / "child"  -> JsonLD.fromEntityId(EntityId.of(schema / childField))
        )
      }
    }
  }

  "asEntityType" should {

    "convert a Property to EntityType" in {
      forAll { property: Property =>
        property.asEntityType shouldBe EntityType.of(property)
      }
    }
  }

  "asEntityId" should {
    "return EntityId for a custom object" in {
      forAll { (id: EntityId, string: String, int: Int) =>
        implicit val encoder: EntityIdEncoder[Object] = EntityIdEncoder.instance { _ =>
          id
        }

        Object(string, int).asEntityId shouldBe id
      }
    }
  }

  private case class Object(string: String, int: Int)
  private case class Parent(string: String, child: Child)
  private case class Child(int: Int)
}
