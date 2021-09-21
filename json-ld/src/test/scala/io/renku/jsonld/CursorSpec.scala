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

package io.renku.jsonld

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.DecodingFailure
import io.renku.jsonld.Cursor.FlattenedArrayCursor
import io.renku.jsonld.JsonLD.JsonLDArray
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.Generators.nonEmptyStrings
import io.renku.jsonld.generators.JsonLDGenerators.{entityIds, entityTypes, entityTypesObject, jsonLDEdges, jsonLDEntities, jsonLDValues, properties, schemas, valuesProperties}
import io.renku.jsonld.syntax._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CursorSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "Empty" should {

    "show as 'Empty Cursor' if does not have a message" in {
      Cursor.Empty().show shouldBe "Empty cursor"
    }

    "show as 'Empty Cursor' with the cause if it has a message" in {
      val message = nonEmptyStrings().generateOne
      Cursor.Empty(message).show shouldBe s"Empty cursor cause by $message"
    }
  }

  "as" should {

    "return success if cursor can be decoded to the requested type" in {
      forAll(
        Gen.oneOf(jsonLDValues, jsonLDEntities, jsonLDEdges, entityIds.map(_.asJsonLD), jsonLDValues.map(JsonLD.arr(_)))
      )(json => json.cursor.as[JsonLD] shouldBe json.asRight[DecodingFailure])
    }
  }

  "getEntityTypes" should {

    "return entity's EntityTypes" in {
      forAll { (id: EntityId, entityTypes: EntityTypes, property: (Property, JsonLD)) =>
        val cursor = JsonLD
          .entity(id, entityTypes, property)
          .cursor

        cursor.getEntityTypes shouldBe entityTypes.asRight[DecodingFailure]
      }
    }

    "return a failure for non-JsonLDEntity objects" in {
      forAll(jsonLDValues) { value =>
        value.cursor.getEntityTypes shouldBe DecodingFailure("No EntityTypes found on non-JsonLDEntity object", Nil)
          .asLeft[EntityTypes]
      }
    }
  }

  "downEntityId" should {

    "return a Cursor pointing to entityId of the given entity" in {
      forAll { (id: EntityId, entityType: EntityType, property: (Property, JsonLD)) =>
        val cursor = JsonLD
          .entity(id, EntityTypes.of(entityType), property)
          .cursor

        cursor.downEntityId.jsonLD shouldBe id.asJsonLD
      }
    }

    "return a Cursor pointing to the entityId object of a property if it's encoded as single-item list of entityIds" in {
      val property      = properties.generateOne
      val childEntityId = entityIds.generateOne
      val cursor = JsonLD
        .entity(entityIds.generateOne, entityTypesObject.generateOne, property -> JsonLD.arr(childEntityId.asJsonLD))
        .cursor

      cursor.downField(property).downEntityId.jsonLD shouldBe childEntityId.asJsonLD
    }

    "return the same Cursor if it's a cursor on JsonLDEntityId" in {
      val cursor = JsonLD.JsonLDEntityId(entityIds.generateOne).cursor
      cursor.downEntityId shouldBe cursor
    }

    forAll(
      Table(
        "case"     -> "List of EntityId",
        "multiple" -> entityIds.generateNonEmptyList(minElements = 2).toList.map(_.asJsonLD),
        "no"       -> List.empty[JsonLD]
      )
    ) { (caseName, entityIdsList) =>
      s"return an empty Cursor when called on a property with $caseName entityIds in an array" in {
        val property = properties.generateOne
        val cursor = JsonLD
          .entity(entityIds.generateOne, entityTypesObject.generateOne, property -> JsonLD.arr(entityIdsList: _*))
          .cursor

        cursor.downField(property).downEntityId shouldBe Cursor.Empty(
          s"Expected @id but got an array of size ${entityIdsList.size}"
        )
      }
    }
  }

  "return an empty Cursor if object is not a JsonLDEntity" in {
    JsonLD.fromInt(Arbitrary.arbInt.arbitrary.generateOne).cursor.downEntityId shouldBe Cursor.Empty(
      "Expected @id but got a JsonLDValue"
    )
  }

  "downType" should {

    "return a Cursor pointing to object of the given type" in {
      forAll { (id: EntityId, entityType: EntityType, schema: Schema, property: (Property, JsonLD)) =>
        val searchedType = (schema / "type").asEntityType
        val cursor = JsonLD
          .entity(id, EntityTypes.of(entityType, searchedType), property)
          .cursor

        cursor.downType(searchedType) shouldBe cursor
      }
    }

    "return an empty Cursor if there is no object with the searched type(s)" in {
      forAll { (id: EntityId, entityTypes: EntityTypes, schema: Schema, property: (Property, JsonLD)) =>
        val searchedType = (schema / "type").asEntityType
        JsonLD
          .entity(id, entityTypes, property)
          .cursor
          .downType(searchedType) shouldBe Cursor.Empty(s"Cannot find entity with $searchedType @type")
      }
    }
  }

  "downArray" should {

    "return itself if called on an array wrapped in the FlattenedArrayCursor" in {
      val cursor =
        FlattenedArrayCursor(Cursor.Empty.noMessage, JsonLDArray(jsonLDValues.generateNonEmptyList().toList), Map.empty)
      cursor.downArray shouldBe cursor
    }

    "return itself if called on an array wrapped in the Cursor" in {
      val array  = JsonLDArray(jsonLDValues.generateNonEmptyList().toList)
      val cursor = Cursor.TopCursor(array)
      cursor.downArray shouldBe Cursor.ArrayCursor(cursor, array)
    }

    "return an empty Cursor if called not on an array" in {
      Cursor.TopCursor(jsonLDValues.generateOne).downArray shouldBe Cursor.Empty(
        "Expected JsonLD Array but got JsonLDValue"
      )
    }
  }

  "downField" should {

    "return a Cursor pointing to a searched property" in {
      forAll { (id: EntityId, entityTypes: EntityTypes, property1: (Property, JsonLD), property2: (Property, JsonLD)) =>
        val (prop1Name, prop1Value) = property1
        val cursor = JsonLD
          .entity(id, entityTypes, property1, property2)
          .cursor
          .downField(prop1Name)

        cursor.as[JsonLD] shouldBe prop1Value.asRight[DecodingFailure]
      }
    }

    "return an empty Cursor if given property cannot be found" in {
      val field = properties.generateOne
      jsonLDEntities.generateOne.cursor.downField(field) shouldBe Cursor.Empty(s"Cannot find $field property")
    }

    "return an empty Cursor if given property points to object that cannot be associated with a property" in {
      val field = properties.generateOne
      jsonLDEntities.generateOne.copy(properties = Map(field -> JsonLD.Null)).cursor.downField(field) shouldBe
        Cursor.Empty(s"$field property points to JsonLDNull")
    }

    "return an empty Cursor if called on neither JsonLDEntity nor JsonLDArray" in {
      val field = properties.generateOne
      JsonLD.Null.cursor.downField(field) shouldBe Cursor.Empty("Expected JsonLD entity or array but got JsonLDNull")
      jsonLDValues.generateOne.cursor.downField(field) shouldBe Cursor.Empty(
        "Expected JsonLD entity or array but got JsonLDValue"
      )
      entityIds.generateOne.asJsonLD.cursor.downField(field) shouldBe Cursor.Empty(
        "Expected JsonLD entity or array but got JsonLDEntityId"
      )
      jsonLDEdges.generateOne.cursor.downField(field) shouldBe Cursor.Empty(
        "Expected JsonLD entity or array but got JsonLDEdge"
      )
    }
  }

  "delete" should {

    "allow to remove a selected field" in {
      forAll { (id: EntityId, entityTypes: EntityTypes, property1: (Property, JsonLD), property2: (Property, JsonLD)) =>
        JsonLD
          .entity(id, entityTypes, property1, property2)
          .cursor
          .downField(property1._1)
          .delete
          .top shouldBe Some(JsonLD.entity(id, entityTypes, property2))
      }
    }

    "allow to remove a selected entity" in {
      forAll { (id: EntityId, entityTypes: EntityTypes, property: (Property, JsonLD)) =>
        JsonLD
          .entity(id, entityTypes, property)
          .cursor
          .downType(entityTypes.toList.head)
          .delete
          .top shouldBe None
      }
    }
  }
}
