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
import io.renku.jsonld.JsonLD.{JsonLDEntity, MalformedJsonLD}
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.JsonLDGenerators._
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class JsonLDMergeSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "merge" should {

    "do nothing for JsonLDValue" in {
      forAll(jsonLDValues) { json =>
        json.merge shouldBe Right(json)
      }
    }

    "do nothing for JsonLDEntityId" in {
      forAll { entityId: EntityId =>
        val json = JsonLD.fromEntityId(entityId)
        json.merge shouldBe Right(json)
      }
    }

    "do nothing for JsonLDNull" in {
      JsonLD.Null.merge shouldBe Right(JsonLD.Null)
    }

    "do nothing for JsonLDEntity" in {
      forAll { entity: JsonLDEntity =>
        entity.merge shouldBe Right(entity)
      }
    }

    "do nothing for JsonLDArray of JsonLDEntities if there are not Edges to merge" in {
      forAll { (entity1: JsonLDEntity, entity2: JsonLDEntity) =>
        JsonLD.arr(entity1, entity2).merge shouldBe Right(JsonLD.arr(entity1, entity2))
      }
    }

    "do nothing for JsonLDArray of JsonLDEntityIds" in {
      forAll { (id1: EntityId, id2: EntityId) =>
        JsonLD.arr(id1.asJsonLD, id2.asJsonLD).merge shouldBe Right(JsonLD.arr(id1.asJsonLD, id2.asJsonLD))
      }
    }

    "do nothing for JsonLDArray of JsonLDValues" in {
      forAll(jsonLDValues, jsonLDValues) { (value1, value2) =>
        JsonLD.arr(value1, value2).merge shouldBe Right(JsonLD.arr(value1, value2))
      }
    }

    "move targets from relevant edges to JsonLDEntities defined in the JsonLDArray" in {
      val entity1 = JsonLD.entity(
        entityIds.generateOne,
        entityTypesObject.generateOne,
        valuesProperties.generateOne
      )
      val entity2 = JsonLD.entity(
        entityIds.generateOne,
        entityTypesObject.generateOne,
        valuesProperties.generateOne
      )
      val entity3 = JsonLD.entity(
        entityIds.generateOne,
        entityTypesObject.generateOne,
        valuesProperties.generateOne
      )
      val edge1 = JsonLD.edge(entity1.id, properties.generateOne, entity2.id)
      val edge2 = JsonLD.edge(entity1.id, edge1.property, entity3.id)
      val edge3 = JsonLD.edge(entity1.id, properties.generateOne, entity3.id)

      val Right(merged) = JsonLD.arr(entity1, entity2, entity3, edge1, edge2, edge3).merge

      merged.asArray.sequence.flatten should contain theSameElementsAs List(
        entity1.copy(properties =
          entity1.properties +
            (edge1.property -> JsonLD.arr(edge1.target.asJsonLD, edge2.target.asJsonLD)) +
            (edge3.property -> edge3.target.asJsonLD)
        ),
        entity2,
        entity3
      )
    }

    "return an MalformedJsonLD error when there are edges entities and something else" in {
      val entity1 = JsonLD.entity(
        entityIds.generateOne,
        entityTypesObject.generateOne,
        valuesProperties.generateOne
      )
      val entity2 = JsonLD.entity(
        entityIds.generateOne,
        entityTypesObject.generateOne,
        valuesProperties.generateOne
      )

      val edge1 = JsonLD.edge(entity1.id, properties.generateOne, entity2.id)

      val Left(error) = JsonLD.arr(entity1, entity2, edge1, jsonLDValues.generateOne).merge

      error shouldBe a[MalformedJsonLD]

      error.getMessage shouldBe "Flattened JsonLD contains illegal objects"
    }

    "return the unchanged entity and edge when there are edges which do not belong to any entities" in {
      val entity1 = JsonLD.entity(
        entityIds.generateOne,
        entityTypesObject.generateOne,
        valuesProperties.generateOne
      )

      val edge = jsonLDEdges.generateOne

      val Right(merged) = JsonLD.arr(entity1, edge).merge

      merged.asArray.sequence.flatten should contain theSameElementsAs List(entity1, edge)

    }

  }
}
