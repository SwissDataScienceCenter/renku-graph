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

import java.time.{Instant, LocalDate}

import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.jsonld.syntax._
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.literal._
import io.circe.parser._
import io.circe.syntax._
import io.renku.jsonld.JsonLD.{JsonLDEntity, JsonLDEntityId, MalformedJsonLD}
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.Generators._
import io.renku.jsonld.generators.JsonLDGenerators._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random

class JsonLDSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "JsonLD.fromString" should {

    "allow to construct JsonLD String value" in {
      forAll { value: String =>
        JsonLD.fromString(value).toJson shouldBe
          json"""{
            "@value": $value
          }"""
      }
    }

    "have no entityId" in {
      forAll { value: String =>
        JsonLD.fromString(value).entityId shouldBe None
      }
    }

    "have no entityTypes" in {
      forAll { value: String =>
        JsonLD.fromString(value).entityTypes shouldBe None
      }
    }
  }

  "JsonLD.fromInt" should {

    "allow to construct JsonLD Int value" in {
      forAll { value: Int =>
        JsonLD.fromInt(value).toJson shouldBe
          json"""{
            "@value": $value
          }"""
      }
    }

    "have no entityId" in {
      forAll { value: Int =>
        JsonLD.fromInt(value).entityId shouldBe None
      }
    }

    "have no entityTypes" in {
      forAll { value: Int =>
        JsonLD.fromInt(value).entityTypes shouldBe None
      }
    }
  }

  "JsonLD.fromLong" should {

    "allow to construct JsonLD Long value" in {
      forAll { value: Long =>
        JsonLD.fromLong(value).toJson shouldBe
          json"""{
            "@value": $value
          }"""
      }
    }

    "have no entityId" in {
      forAll { value: Long =>
        JsonLD.fromLong(value).entityId shouldBe None
      }
    }

    "have no entityTypes" in {
      forAll { value: Long =>
        JsonLD.fromLong(value).entityTypes shouldBe None
      }
    }
  }

  "JsonLD.fromInstant" should {

    "allow to construct JsonLD xsd:dateTime value" in {
      forAll { value: Instant =>
        JsonLD.fromInstant(value).toJson shouldBe
          json"""{
            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
            "@value": $value
          }"""
      }
    }

    "have no entityId" in {
      forAll { value: Instant =>
        JsonLD.fromInstant(value).entityId shouldBe None
      }
    }

    "have no entityTypes" in {
      forAll { value: Instant =>
        JsonLD.fromInstant(value).entityTypes shouldBe None
      }
    }
  }

  "JsonLD.fromLocalDate" should {

    "allow to construct JsonLD xsd:dateTime value" in {
      forAll { value: LocalDate =>
        JsonLD.fromLocalDate(value).toJson shouldBe
          json"""{
            "@type": "http://schema.org/Date",
            "@value": $value
          }"""
      }
    }

    "have no entityId" in {
      forAll { value: LocalDate =>
        JsonLD.fromLocalDate(value).entityId shouldBe None
      }
    }

    "have no entityTypes" in {
      forAll { value: LocalDate =>
        JsonLD.fromLocalDate(value).entityTypes shouldBe None
      }
    }
  }

  "JsonLD.fromBoolean" should {

    "allow to construct JsonLD Boolean value" in {
      forAll { value: Boolean =>
        JsonLD.fromBoolean(value).toJson shouldBe
          json"""{
            "@value": $value
          }"""
      }
    }

    "have no entityId" in {
      forAll { value: Boolean =>
        JsonLD.fromBoolean(value).entityId shouldBe None
      }
    }

    "have no entityTypes" in {
      forAll { value: Boolean =>
        JsonLD.fromBoolean(value).entityTypes shouldBe None
      }
    }
  }

  "JsonLD.Null" should {

    "be Json.Null" in {
      JsonLD.JsonLDNull.toJson == null
    }

    "have no entityId" in {
      JsonLD.JsonLDNull.entityId shouldBe None
    }

    "have no entityTypes" in {
      JsonLD.JsonLDNull.entityTypes shouldBe None
    }
  }

  "JsonLD.fromOption" should {

    "allow to construct JsonLD value when the value is present" in {
      forAll { value: Long =>
        JsonLD.fromOption(Some(value)).toJson shouldBe
          json"""{
            "@value": $value
          }"""
      }
    }

    "allow to construct JsonLD value when the value is present and has some type" in {
      forAll { value: Instant =>
        implicit val jsonLDEncoder: JsonLDEncoder[Instant] = JsonLDEncoder.instance[Instant](JsonLD.fromInstant)
        JsonLD.fromOption(Some(value)).toJson shouldBe JsonLD.fromInstant(value).toJson
      }
    }

    "allow to construct JsonLD value when the value is absent" in {
      JsonLD.fromOption(Option.empty[String]) shouldBe JsonLD.JsonLDNull
    }

    "have entityId for Some entity having one" in {
      forAll { (id: EntityId, types: EntityTypes, property: Property, value: String) =>
        implicit val encoder: JsonLDEncoder[Object] = JsonLDEncoder.instance { o: Object =>
          JsonLD.entity(id, types, property -> JsonLD.fromString(o.value))
        }

        JsonLD.fromOption(Some(Object(value))).entityId shouldBe Some(id)
      }
    }

    "have no entityId for None" in {
      JsonLD.fromOption(Option.empty[String]).entityId shouldBe None
    }

    "have entityTypes for Some entity having one" in {
      forAll { (id: EntityId, types: EntityTypes, property: Property, value: String) =>
        implicit val encoder: JsonLDEncoder[Object] = JsonLDEncoder.instance { o: Object =>
          JsonLD.entity(id, types, property -> JsonLD.fromString(o.value))
        }

        JsonLD.fromOption(Some(Object(value))).entityTypes shouldBe Some(types)
      }
    }

    "have no entityTypes for None" in {
      JsonLD.fromOption(Option.empty[String]).entityTypes shouldBe None
    }
  }

  "JsonLD.fromEntityId" should {

    "allow to construct JsonLD EntityId object" in {
      forAll { value: EntityId =>
        JsonLD.fromEntityId(value).toJson shouldBe
          json"""{
            "@id": $value
          }"""
      }
    }

    "have no entityId" in {
      forAll { value: EntityId =>
        JsonLD.fromEntityId(value).entityId shouldBe None
      }
    }

    "have no entityTypes" in {
      forAll { value: EntityId =>
        JsonLD.fromEntityId(value).entityTypes shouldBe None
      }
    }
  }

  "JsonLD.arr" should {

    "allow to construct an array of JsonLD objects" in {
      forAll { values: List[String] =>
        JsonLD.arr(values map JsonLD.fromString: _*).toJson shouldBe
          Json.arr(values map Json.fromString map (json => Json.obj("@value" -> json)): _*)
      }
    }

    "have no entityId" in {
      forAll { values: List[String] =>
        JsonLD.arr(values map JsonLD.fromString: _*).entityId shouldBe None
      }
    }

    "have no entityTypes" in {
      forAll { values: List[String] =>
        JsonLD.arr(values map JsonLD.fromString: _*).entityTypes shouldBe None
      }
    }
  }

  "JsonLD.entity" should {

    "allow to construct JsonLD entity with multiple types" in {
      forAll(entityIds, nonEmptyList(entityTypes, minElements = 2), schemas) { (id, types, schema) =>
        val (name1, values1)            = listValueProperties(schema).generateOne
        val property1 @ (_, value1)     = name1 -> JsonLD.arr(values1.toList: _*)
        val property2 @ (name2, value2) = singleValueProperties(schema).generateOne

        JsonLD.entity(id, EntityTypes(types), property1, property2).toJson shouldBe
          parse(s"""{
            "@id": ${id.asJson},
            "@type": ${Json.arr(types.toList.map(_.asJson): _*)},
            "$name1": ${value1.toJson},
            "$name2": ${value2.toJson}
          }""").fold(throw _, identity)
      }
    }

    "allow to construct JsonLD entity with single type" in {
      forAll(entityIds, entityTypes, schemas) { (id, entityType, schema) =>
        val (name1, values1)            = listValueProperties(schema).generateOne
        val property1 @ (_, value1)     = name1 -> JsonLD.arr(values1.toList: _*)
        val property2 @ (name2, value2) = singleValueProperties(schema).generateOne

        JsonLD.entity(id, EntityTypes.of(entityType), property1, property2).toJson shouldBe
          parse(s"""{
            "@id": ${id.asJson},
            "@type": ${entityType.asJson},
            "$name1": ${value1.toJson},
            "$name2": ${value2.toJson}
          }""").fold(throw _, identity)
      }
    }

    "skip properties with properties having None values" in {
      forAll { (id: EntityId, entityType: EntityType, schema: Schema) =>
        val property1 @ (name1, value1) = singleValueProperties(schema).generateOne
        val property2                   = properties.generateOne -> JsonLD.fromOption(Option.empty[String])

        JsonLD.entity(id, EntityTypes.of(entityType), property1, property2).toJson shouldBe
          parse(s"""{
            "@id": ${id.asJson},
            "@type": ${entityType.asJson},
            "$name1": ${value1.toJson}
          }""").fold(throw _, identity)
      }
    }

    "be able to add a single reverse property" in {
      forAll {
        (parentId:       EntityId,
         parentTypes:    EntityTypes,
         parentProperty: (Property, JsonLD),
         childId:        EntityId,
         childTypes:     EntityTypes,
         childProperty:  (Property, JsonLD)
        ) =>
          val reverseProperty: Property = properties.generateOne
          val child = JsonLD.entity(childId, childTypes, childProperty)
          JsonLD
            .entity(parentId, parentTypes, Reverse.ofEntities(reverseProperty -> child), parentProperty)
            .toJson shouldBe
            parse(s"""{
            "@id":                  ${parentId.asJson},
            "@type":                ${parentTypes.asJson},
            "${parentProperty._1}": ${parentProperty._2.toJson},
            "@reverse":             {
              "$reverseProperty": {
                "@id":                 ${childId.asJson},
                "@type":               ${childTypes.asJson},
                "${childProperty._1}": ${childProperty._2.toJson}
              }
            }
          }""").fold(throw _, identity)
      }
    }

    "be able to add multiple reverse properties" in {
      forAll {
        (parentId:       EntityId,
         parentTypes:    EntityTypes,
         parentProperty: (Property, JsonLD),
         childId:        EntityId,
         childTypes:     EntityTypes,
         childProperty:  (Property, JsonLD)
        ) =>
          val reverseProperty1: Property = properties.generateOne
          val reverseProperty2: Property = properties.generateOne
          val child = JsonLD.entity(childId, childTypes, childProperty)
          JsonLD
            .entity(parentId,
                    parentTypes,
                    Reverse.ofEntities(reverseProperty1 -> child, reverseProperty2 -> child),
                    parentProperty
            )
            .toJson shouldBe
            parse(s"""{
            "@id":                  ${parentId.asJson},
            "@type":                ${parentTypes.asJson},
            "${parentProperty._1}": ${parentProperty._2.toJson},
            "@reverse": {
              "$reverseProperty1": {
                "@id":                 ${childId.asJson},
                "@type":               ${childTypes.asJson},
                "${childProperty._1}": ${childProperty._2.toJson}
              },
              "$reverseProperty2": {
                "@id":                 ${childId.asJson},
                "@type":               ${childTypes.asJson},
                "${childProperty._1}": ${childProperty._2.toJson}
              }
            }
          }""").fold(throw _, identity)
      }
    }

    "be able to add reverse property with a list of entities" in {
      forAll { (parentId: EntityId, parentTypes: EntityTypes, parentProperty: (Property, JsonLD)) =>
        val reverseProperty         = properties.generateOne
        val reversePropertyEntities = jsonLDEntities.generateNonEmptyList().toList
        val Right(reverse)          = Reverse.of(reverseProperty -> reversePropertyEntities)
        JsonLD
          .entity(parentId, parentTypes, reverse, parentProperty)
          .toJson shouldBe
          parse(s"""{
            "@id":                  ${parentId.asJson},
            "@type":                ${parentTypes.asJson},
            "${parentProperty._1}": ${parentProperty._2.toJson},
            "@reverse":             {
              "$reverseProperty": ${Json.arr(reversePropertyEntities.map(_.toJson): _*)}
            }
          }""").fold(throw _, identity)
      }
    }

    "be able to skip reverse property when empty" in {
      forAll { (parentId: EntityId, parentTypes: EntityTypes, parentProperty: (Property, JsonLD)) =>
        JsonLD
          .entity(parentId, parentTypes, Reverse.empty, parentProperty)
          .toJson shouldBe
          parse(s"""{
            "@id":                  ${parentId.asJson},
            "@type":                ${parentTypes.asJson},
            "${parentProperty._1}": ${parentProperty._2.toJson}
          }""").fold(throw _, identity)
      }
    }

    "have some entityId" in {
      forAll { (id: EntityId, types: EntityTypes, property: (Property, JsonLD)) =>
        JsonLD.entity(id, types, property).entityId shouldBe Some(id)
      }
    }

    "have some entityTypes" in {
      forAll { (id: EntityId, types: EntityTypes, property: (Property, JsonLD)) =>
        JsonLD.entity(id, types, property).entityTypes shouldBe Some(types)
      }
    }
  }

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

  "asArray" should {
    "return vector with single JsonLDValue" in {
      forAll(jsonLDValues) { json =>
        json.asArray shouldBe Some(Vector(json))
      }
    }

    "return vector with single JsonLDEntityID" in {
      forAll { entityId: EntityId =>
        val idAsJson = JsonLD.fromEntityId(entityId)
        idAsJson.asArray shouldBe Some(Vector(idAsJson))
      }
    }

    "return vector containing a null" in {
      JsonLD.Null.asArray shouldBe Some(Vector(JsonLD.Null))
    }

    "return vector containing the entity" in {
      forAll(jsonLDEntities) { entity =>
        entity.asArray shouldBe Some(Vector(entity))
      }
    }

    "return a vector with the same elements as the array" in {
      forAll(nonEmptyList(jsonLDEntities)) { entities =>
        JsonLD.arr(entities.toList: _*).asArray shouldBe Some(entities.toList.toVector)
      }
    }
  }

  private def listValueProperties(schema: Schema): Gen[(Property, NonEmptyList[JsonLD])] =
    for {
      property <- nonBlankStrings() map (p => schema / p.value)
      valuesGen = nonBlankStrings() map (v => JsonLD.fromString(v.value))
      values <- nonEmptyList(valuesGen, minElements = 2)
    } yield property -> values

  private def singleValueProperties(schema: Schema): Gen[(Property, JsonLD)] =
    for {
      property <- nonBlankStrings() map (p => schema / p.value)
      value    <- nonBlankStrings() map (v => JsonLD.fromString(v.value))
    } yield property -> value

  private lazy val entityProperties: Gen[(Property, JsonLDEntity)] = for {
    property <- properties
    entity   <- jsonLDEntities
  } yield property -> entity

  private case class Object(value: String)

  private implicit class JsonLDEntityOps(entity: JsonLDEntity) {

    def add(properties: List[(Property, JsonLD)]): JsonLDEntity =
      properties.foldLeft(entity) { case (entity, (property, entityValue)) =>
        entity.add(property -> entityValue)
      }

    def add(property: (Property, JsonLD)): JsonLDEntity = entity.copy(properties = entity.properties :+ property)
  }

  private implicit class EitherJsonLDOps[T <: Exception, U](either: Either[T, U]) {
    lazy val unsafeGetRight: U = either.fold(throw _, identity)
  }

}
