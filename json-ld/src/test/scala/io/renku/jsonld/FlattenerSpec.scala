package io.renku.jsonld

import cats.data.NonEmptyList
import io.renku.jsonld.JsonLD.{JsonLDEntity, JsonLDEntityId, MalformedJsonLD}
import io.renku.jsonld.generators.Generators.Implicits.asArbitrary
import io.renku.jsonld.generators.JsonLDGenerators.{jsonLDEntities, jsonLDValues, properties, valuesProperties}
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import cats.syntax.all._
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.JsonLDGenerators._
import io.renku.jsonld.syntax._
import scala.util.Random
import eu.timepit.refined.auto._

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
        val entityAsJsonLD = JsonLD.entity(id, types, property1, other: _*)
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
        val newProperties   = valuesProperties.generateNonEmptyList()
        val parent0         = jsonLDEntities.generateOne
        val parent1         = jsonLDEntities.generateOne
        val modifiedParent0 = parent0.copy(properties = newProperties)
        val parent1WithModifiedChild =
          parent1.copy(properties = parent1.properties :+ (properties.generateOne -> modifiedParent0))

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
        val newProperties  = valuesProperties.generateNonEmptyList()
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

    "do nothing for JsonLDArray if all its items do not have nested entities" in {
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

        val arrayAfterFlattening = JsonLD
          .arr(entityWithChildren)
          .flatten
          .unsafeGetRight
          .asArray
          .get

        val expected = Vector(child, entityWithChildrenFlattened)

        arrayAfterFlattening should contain theSameElementsAs expected
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

      def replaceEntityProperty(properties: NonEmptyList[(Property, JsonLD)],
                                property:   Property,
                                entity:     JsonLDEntity
      ) =
        NonEmptyList(property -> JsonLDEntityId(entity.id),
                     properties.filterNot { case (_, item) =>
                       item == entity
                     }
        )

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

          JsonLD
            .arr(nestedParent0, nestedParent1)
            .flatten
            .map(_.asArray.get)
            .unsafeGetRight should contain theSameElementsAs
            Vector(child, deNestedParent0, deNestedParent1, deNestedParent2)
      }
    }

    "deduplicate pulled out entities which contain an array property with elements in a different order" in {
      forAll { (property: Property, entity: JsonLDEntity) =>
        val normalValues: List[JsonLD] = jsonLDValues.generateNonEmptyList(minElements = 2).toList
        val mixedUpValues = Random.shuffle(normalValues)
        val normalEntity  = entity.copy(properties = entity.properties :+ (property, JsonLD.arr(normalValues: _*)))
        val mixedUpEntity = entity.copy(properties = entity.properties :+ (property, JsonLD.arr(mixedUpValues: _*)))

        JsonLD.arr(normalEntity, mixedUpEntity).flatten.unsafeGetRight.asArray.get.size == 1
      }
    }

    "pull out child entities from reversed property of an entity" in {
      forAll { (entity0: JsonLDEntity, child0: JsonLDEntity, child1: JsonLDEntity, reverseProperty: Property) =>
        val children = List(child0, child1)
        val parent: JsonLDEntity =
          entity0.copy(reverse = Reverse.of((reverseProperty, children)).unsafeGetRight)

        val expectedReverse =
          Reverse.of((reverseProperty, children.map(child => JsonLDEntityId(child.id)))).unsafeGetRight

        val parentWithIdsOfChildren = parent.copy(reverse = expectedReverse)

        parent.flatten.map(_.asArray) shouldBe Some(Vector(parentWithIdsOfChildren, child0, child1)).asRight
      }
    }
  }

  private lazy val entityProperties: Gen[(Property, JsonLDEntity)] = for {
    property <- properties
    entity   <- jsonLDEntities
  } yield property -> entity

  private implicit class EitherJsonLDOps[T <: Exception, U](either: Either[T, U]) {
    lazy val unsafeGetRight: U = either.fold(throw _, identity)
  }

  private implicit class JsonLDEntityOps(entity: JsonLDEntity) {

    def add(properties: List[(Property, JsonLD)]): JsonLDEntity =
      properties.foldLeft(entity) { case (entity, (property, entityValue)) =>
        entity.add(property -> entityValue)
      }

    def add(property: (Property, JsonLD)): JsonLDEntity = entity.copy(properties = entity.properties :+ property)
  }

}
