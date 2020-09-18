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
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.literal._
import io.circe.parser._
import io.circe.syntax._
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.Generators._
import io.renku.jsonld.generators.JsonLDGenerators._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

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

  private case class Object(value: String)
}
