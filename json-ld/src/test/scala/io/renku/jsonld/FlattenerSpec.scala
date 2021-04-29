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

import eu.timepit.refined.auto._
import io.renku.jsonld.JsonLD.{JsonLDEntity, JsonLDEntityId, MalformedJsonLD}
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.JsonLDGenerators._
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random

class FlattenerSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "flatten" should {

    "do nothing for JsonLDValue" in {
      forAll(jsonLDValues) { json =>
        json.flatten shouldBe Right(json)
      }
    }

    "do nothing for JsonLDEntityId" in {
      forAll { entityId: EntityId =>
        val json = JsonLD.fromEntityId(entityId)
        json.flatten shouldBe Right(json)
      }
    }

    "do nothing for JsonLDNull" in {
      JsonLD.Null.flatten shouldBe Right(JsonLD.Null)
    }

    "do nothing if JsonLDEntity does not have nested object in its properties" in {
      forAll { (id: EntityId, types: EntityTypes, property1: (Property, JsonLD), other: List[(Property, JsonLD)]) =>
        val entityAsJsonLD = JsonLD.arr(JsonLD.entity(id, types, property1, other: _*))
        entityAsJsonLD.flatten shouldBe Right(entityAsJsonLD)
      }
    }

    "pull out all the nested JsonLDEntity entities into a flat JsonLDArray " +
      "and replace the nested properties values with relevant EntityIds" in {
        /*
      grandparent
         |
      parent
        |
      child
         |
         |
         V
(child, parent, grandparent)  // order not guaranteed

         */
        forAll {
          (childlessGrandparent: JsonLDEntity, parentRelationProperty: Property, parentNotNested: JsonLDEntity) =>
            val childrenTuples       = entityProperties.generateNonEmptyList().toList
            val parentWithProperties = parentNotNested.add(childrenTuples)
            val grandparentWithChild = childlessGrandparent.add(parentRelationProperty -> parentWithProperties)

            val flattenedGrandparent =
              childlessGrandparent.add(parentRelationProperty -> JsonLD.fromEntityId(parentWithProperties.id))
            val flattenedParent = childrenTuples
              .foldLeft(parentNotNested) { case (parent, (property, child)) =>
                parent.add(property -> child.id.asJsonLD)
              }
            val children = childrenTuples.map { case (_, entity) => entity: JsonLD }

            val Right(actual) = grandparentWithChild.flatten
            actual.toJson.asArray.get should contain theSameElementsAs {
              JsonLD.arr(flattenedGrandparent +: flattenedParent +: children: _*).toJson.asArray.get
            }
        }
      }

    "should fail if there are two unequal child entities with the same EntityID in a single Entity" in {
      forAll { (grandParent: JsonLDEntity) =>
        val newProperties   = valuesProperties.generateNonEmptyMap()
        val parent0         = jsonLDEntities.generateOne
        val parent1         = jsonLDEntities.generateOne
        val modifiedParent0 = parent0.copy(properties = newProperties)
        val parent1WithModifiedChild =
          parent1.copy(properties = parent1.properties + (properties.generateOne -> modifiedParent0))

        val grandParentWithModifiedChildren =
          grandParent.add(List(properties.generateOne -> parent0, properties.generateOne -> parent1WithModifiedChild))

        grandParentWithModifiedChildren.flatten shouldBe Left(
          MalformedJsonLD("Some entities share an ID even though they're not the same")
        )
      }

    }

    "should fail if there are two unequal entities with the same EntityId in the nested structure" in {
      forAll { (parent0: JsonLDEntity, parent1: JsonLDEntity) =>
        val childrenTuples = entityProperties.generateNonEmptyList().toList
        val newProperties  = valuesProperties.generateNonEmptyMap()
        val modifiedChild = childrenTuples.head match {
          case (property, entity) =>
            (property, entity.copy(properties = newProperties))
        }
        val childrenWithModified        = modifiedChild +: childrenTuples.tail
        val parent0WithNormalChildren   = parent0.add(childrenTuples)
        val parent1WithModifiedChildren = parent1.add(childrenWithModified)

        JsonLD.arr(parent0WithNormalChildren, parent1WithModifiedChildren).flatten shouldBe Left(
          MalformedJsonLD("Some entities share an ID even though they're not the same")
        )
      }
    }

    "do nothing for JsonLDArray if none of its items have nested entities" in {
      forAll { (entity0: JsonLDEntity, entity1: JsonLDEntity) =>
        val entity0WithEntity1IdAsProperty = entity0.add(properties.generateOne -> entity1.id.asJsonLD)
        val arrayOfEntities                = JsonLD.arr(entity0WithEntity1IdAsProperty, entity1)
        arrayOfEntities.flatten shouldBe Right(arrayOfEntities)
      }
    }

    "pull out nested entities from JsonLDArray items and put them on the root level" in {
      forAll { (entity: JsonLDEntity, child: JsonLDEntity) =>
        val property                    = properties.generateOne
        val entityWithChildren          = entity.add(property -> child)
        val entityWithChildrenFlattened = entity.add(property -> child.id.asJsonLD)

        JsonLD.arr(entityWithChildren).flatten shouldBe Right(JsonLD.arr(child, entityWithChildrenFlattened))
      }
    }

    "de-duplicate same children found in multiple entities" in {
      /*
      (parent0, parent1)
        |         |
      child     parent2
                  |
                child

             |
             |
             V

     (child, parent0, parent2, parent1)  // order not guaranteed

       */

      def replaceEntityProperty(properties: Map[Property, JsonLD], property: Property, entity: JsonLDEntity) =
        properties.removed(property) + (property -> JsonLDEntityId(entity.id))

      forAll {
        (entity0:  JsonLDEntity,
         entity1:  JsonLDEntity,
         entity2:  JsonLDEntity,
         child:    JsonLDEntity,
         property: Property
        ) =>
          val nestedParent0 = entity0.add(property -> child)
          val nestedParent2 = entity2.add(property -> child)
          val nestedParent1 = entity1.add(property -> nestedParent2)

          val deNestedParent0Properties = replaceEntityProperty(nestedParent0.properties, property, child)
          val deNestedParent0           = nestedParent0.copy(properties = deNestedParent0Properties)

          val deNestedParent1Properties = replaceEntityProperty(nestedParent1.properties, property, nestedParent2)
          val deNestedParent1           = nestedParent1.copy(properties = deNestedParent1Properties)

          val deNestedParent2Properties = replaceEntityProperty(nestedParent2.properties, property, child)
          val deNestedParent2           = nestedParent2.copy(properties = deNestedParent2Properties)
          JsonLD.arr(nestedParent0, nestedParent1).flatten shouldBe
            Right(JsonLD.arr(child, deNestedParent0, deNestedParent1, deNestedParent2))
      }
    }

    "deduplicate pulled out entities which contain an array property with elements in a different order" in {
      /*
        JsonLDArray(entity0,       entity1)
          |                         |
        properties(1,2)        properties(2,1)

                       |
                       |
                       V

                  (entity0)        // or entity1
       */
      forAll { (property: Property, entity: JsonLDEntity) =>
        val normalValues: List[JsonLD] = jsonLDValues.generateNonEmptyList(minElements = 2).toList
        val mixedUpValues = Random.shuffle(normalValues)
        val normalEntity  = entity.copy(properties = entity.properties + (property -> JsonLD.arr(normalValues: _*)))
        val mixedUpEntity = entity.copy(properties = entity.properties + (property -> JsonLD.arr(mixedUpValues: _*)))
        JsonLD.arr(normalEntity, mixedUpEntity).flatten should (be(Right(JsonLD.arr(normalEntity))) or be(
          Right(JsonLD.arr(mixedUpEntity))
        ))
      }
    }

    "pull out child entities from reversed property of an entity" in {
      /*
      parent
        |
      Reversed
         |
      (property0 -> (child0, child1))

         |
         |
         V

      (child0, child1, parentDeNested)
       */
      forAll { (entity0: JsonLDEntity, child0: JsonLDEntity, child1: JsonLDEntity, reverseProperty: Property) =>
        val children                       = List(child0, child1)
        val Right(parentReverseProperties) = Reverse.of((reverseProperty, children))
        val parent: JsonLDEntity = entity0.copy(reverse = parentReverseProperties)

        val Right(expectedReverse) = Reverse.of((reverseProperty, children.map(child => JsonLDEntityId(child.id))))

        val parentWithIdsOfChildren = parent.copy(reverse = expectedReverse)
        parent.flatten shouldBe Right(JsonLD.arr(parentWithIdsOfChildren, child0, child1))
      }
    }

    "pull out child entities from reverse properties with two levels of nesting" in {
      /*
        parent0
          |
        Reverse
          |
        (property0 -> (parent1))
                         |
                      Reverse
                         |
                      (property1 -> child2)
       */

      forAll {
        (entity0:   JsonLDEntity,
         property0: Property,
         entity1:   JsonLDEntity,
         property1: Property,
         child2:    JsonLDEntity
        ) =>
          val Right(parent1Reverse) = Reverse.of((property1, child2))
          val parent1               = entity1.copy(reverse = parent1Reverse)
          val Right(parent0Reverse) = Reverse.of((property0, parent1))
          val parent0               = entity0.copy(reverse = parent0Reverse)

          val Right(parent1ReverseFlattened) = Reverse.of((property1, JsonLDEntityId(child2.id)))
          val parent1Flattened               = entity1.copy(reverse = parent1ReverseFlattened)
          val Right(parent0ReverseFlattened) = Reverse.of((property0, JsonLDEntityId(parent1.id)))
          val parent0Flattened               = entity0.copy(reverse = parent0ReverseFlattened)

          parent0.flatten shouldBe Right(JsonLD.arr(parent0Flattened, parent1Flattened, child2))

      }
    }

    "produce the same result the second time as the first time called on the previous result" in {
      forAll { (id: EntityId, types: EntityTypes, property1: (Property, JsonLD), other: List[(Property, JsonLD)]) =>
        val entityAsJsonLD = JsonLD.arr(JsonLD.entity(id, types, property1, other: _*))
        entityAsJsonLD.flatten.flatMap(_.flatten) shouldBe Right(entityAsJsonLD)
      }
    }
  }

  "unsafeFlatten" should {
    "throw an error in the case of MalformedJsonLD" in {
      val parent0        = jsonLDEntities.generateOne
      val parent1        = jsonLDEntities.generateOne
      val childrenTuples = entityProperties.generateNonEmptyList().toList
      val newProperties  = valuesProperties.generateNonEmptyMap()
      val modifiedChild = childrenTuples.head match {
        case (property, entity) =>
          (property, entity.copy(properties = newProperties))
      }
      val childrenWithModified        = modifiedChild +: childrenTuples.tail
      val parent0WithNormalChildren   = parent0.add(childrenTuples)
      val parent1WithModifiedChildren = parent1.add(childrenWithModified)

      intercept[MalformedJsonLD] {
        JsonLD.arr(parent0WithNormalChildren, parent1WithModifiedChildren).flatten.fold(throw _, identity)
      }.getMessage shouldBe "Some entities share an ID even though they're not the same"
    }

  }

  private lazy val entityProperties: Gen[(Property, JsonLDEntity)] = for {
    property <- properties
    entity   <- jsonLDEntities
  } yield property -> entity

  private implicit class JsonLDEntityOps(entity: JsonLDEntity) {

    def add(properties: List[(Property, JsonLD)]): JsonLDEntity =
      properties.foldLeft(entity) { case (entity, (property, entityValue)) =>
        entity.add(property -> entityValue)
      }

    def add(property: (Property, JsonLD)): JsonLDEntity = entity.copy(properties = entity.properties + property)
  }

}
