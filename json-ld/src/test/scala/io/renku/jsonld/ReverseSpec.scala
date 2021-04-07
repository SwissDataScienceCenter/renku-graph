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

import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.renku.jsonld.JsonLD.{JsonLDEntity, JsonLDEntityId}
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.Generators._
import io.renku.jsonld.generators.JsonLDGenerators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ReverseSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "of(Property -> List[JsonLD])" should {

    "return right if all the given items are JsonLDEntities" in {
      forAll(properties, nonEmptyList(jsonLDEntities)) { (property, entities) =>
        val Right(entity) = Reverse.of(property -> entities.toList)

        entity.asJson shouldBe json"""{
          ${property.url}: ${Json.arr(entities.map(_.toJson).toList: _*)}
        }"""
      }
    }

    "return left if at least one of the given items is not JsonLDEntity" in {
      forAll { (property: Property, entities: List[JsonLDEntity], nonEntity: String) =>
        val Left(exception) = Reverse.of(property -> (entities :+ JsonLD.fromString(nonEntity)))

        exception            shouldBe an[IllegalArgumentException]
        exception.getMessage shouldBe s""""@reverse" "$property" property can exist on entity only"""
      }
    }

    "return Reverse.emtpy if an empty list is passed in" in {
      Reverse.of(properties.generateOne -> List.empty) shouldBe Right(Reverse.empty)
    }
  }

  "of varargs of Property to JsonLD" should {

    "return an instance of Reverse if the given properties' values are JsonLDEntities" in {
      forAll(properties, jsonLDEntities, properties, jsonLDEntities) { (property1, entity1, property2, entity2) =>
        val Right(reverse) = Reverse.of(
          property1 -> entity1,
          property2 -> entity2
        )

        reverse.asJson shouldBe json"""{
          ${property1.url}: ${entity1.toJson},
          ${property2.url}: ${entity2.toJson}
        }"""
      }
    }

    "return an instance of Reverse if the given properties' values are JsonLDArrays of JsonLDEntities" in {
      forAll(properties, nonEmptyList(jsonLDEntities)) { (property, entities) =>
        val arrayValue = JsonLD.arr(entities.toList: _*)
        val Right(reverse) = Reverse.of(
          property -> arrayValue
        )

        reverse.asJson shouldBe json"""{
          ${property.url}: ${arrayValue.toJson}
        }"""
      }
    }

    "return left if there are properties with values neither JsonLDEntities nor arrays of JsonLDEntities" in {
      forAll(properties, jsonLDEntities, properties, jsonLDValues, properties, jsonLDEntities) {
        (property1, entity1, property2, value2, property3, entity3) =>
          val Left(exception) = Reverse.of(
            property1 -> entity1,
            property2 -> value2,
            property3 -> entity3
          )

          exception            shouldBe an[IllegalArgumentException]
          exception.getMessage shouldBe s""""@reverse" "$property2" property has to exist on an object"""
      }
    }

    "return left if there are properties with values which are arrays of not JsonLDEntities" in {
      forAll(properties, nonEmptyList(jsonLDEntities), properties, nonEmptyList(jsonLDValues)) {
        (property1, entities, property2, values) =>
          val Left(exception) = Reverse.of(
            property1 -> JsonLD.arr(entities.toList: _*),
            property2 -> JsonLD.arr(values.toList: _*)
          )

          exception            shouldBe an[IllegalArgumentException]
          exception.getMessage shouldBe s""""@reverse" "$property2" property has to exist on each object of an array"""
      }
    }
  }

  "ofJsonLDsUnsafe" should {

    "return an instance of Reverse if the given properties' values are JsonLDEntities" in {
      forAll(properties, jsonLDEntities, properties, jsonLDEntities) { (property1, entity1, property2, entity2) =>
        Reverse
          .ofJsonLDsUnsafe(
            property1 -> entity1,
            property2 -> entity2
          )
          .asJson shouldBe json"""{
          ${property1.url}: ${entity1.toJson},
          ${property2.url}: ${entity2.toJson}
        }"""
      }
    }

    "fail if there are properties with values neither JsonLDEntities nor arrays of JsonLDEntities" in {
      forAll(properties, jsonLDEntities, properties, jsonLDValues, properties, jsonLDEntities) {
        (property1, entity1, property2, value2, property3, entity3) =>
          val exception = intercept[Exception] {
            Reverse.ofJsonLDsUnsafe(
              property1 -> entity1,
              property2 -> value2,
              property3 -> entity3
            )
          }

          exception            shouldBe an[IllegalArgumentException]
          exception.getMessage shouldBe s""""@reverse" "$property2" property has to exist on an object"""
      }
    }
  }

  "fromList" should {

    "return Reverse.empty if an empty list is given" in {
      Reverse.fromList(Nil) shouldBe Right(Reverse.empty)
    }

    "return an instance of Reverse if the given properties' values are JsonLDEntities" in {
      forAll(properties, jsonLDEntities, properties, jsonLDEntities) { (property1, entity1, property2, entity2) =>
        val Right(reverse) = Reverse.fromList(
          List(
            property1 -> entity1,
            property2 -> entity2
          )
        )

        reverse.asJson shouldBe json"""{
          ${property1.url}: ${entity1.toJson},
          ${property2.url}: ${entity2.toJson}
        }"""
      }
    }

    "return an instance of Reverse if the given properties' values are JsonLDArrays of JsonLDEntities" in {
      forAll(properties, nonEmptyList(jsonLDEntities)) { (property, entities) =>
        val arrayValue = JsonLD.arr(entities.toList: _*)
        val Right(reverse) = Reverse.fromList(
          List(property -> arrayValue)
        )

        reverse.asJson shouldBe json"""{
          ${property.url}: ${arrayValue.toJson}
        }"""
      }
    }

    "return an instance of Reverse if the given properties' values are JsonLDArrays of JsonLDEntityIds" in {
      forAll(properties, nonEmptyList(entityIds)) { (property, entityIds) =>
        val arrayValue = JsonLD.arr(entityIds.map(id => JsonLDEntityId(id)).toList: _*)
        val Right(reverse) = Reverse.fromList(
          List(property -> arrayValue)
        )

        reverse.asJson shouldBe json"""{
          ${property.url}: ${arrayValue.toJson}
        }"""
      }
    }

    "return left if there are properties with values neither JsonLDEntities nor arrays of JsonLDEntities nor JsonLDEntityIds" in {
      forAll(properties, jsonLDEntities, properties, jsonLDValues, properties, entityIds) {
        (property1, entity1, property2, value2, property3, entityId) =>
          val Left(exception) = Reverse.fromList(
            List(
              property1 -> entity1,
              property2 -> value2,
              property3 -> JsonLDEntityId(entityId)
            )
          )

          exception            shouldBe an[IllegalArgumentException]
          exception.getMessage shouldBe s""""@reverse" "$property2" property has to exist on an object"""
      }
    }

    "return left if there are properties with values which are arrays of not JsonLDEntities" in {
      forAll(properties, nonEmptyList(jsonLDEntities), properties, nonEmptyList(jsonLDValues)) {
        (property1, entities, property2, values) =>
          val Left(exception) = Reverse.fromList(
            List(
              property1 -> JsonLD.arr(entities.toList: _*),
              property2 -> JsonLD.arr(values.toList: _*)
            )
          )

          exception            shouldBe an[IllegalArgumentException]
          exception.getMessage shouldBe s""""@reverse" "$property2" property has to exist on each object of an array"""
      }
    }
  }

  "fromListUnsafe" should {

    "return an instance of Reverse if all the given items are JsonLDEntities or JsonLDIds" in {
      forAll(properties, nonEmptyList(jsonLDEntities), properties, jsonLDEntities, nonEmptyList(entityIds)) {
        (property1, entities, property2, entity, entityIds) =>
          val jsonLDEntityIds = entityIds.map(JsonLDEntityId(_))
          val properties = List(
            property1 -> JsonLD.arr(entities.toList ++ jsonLDEntityIds.toList: _*),
            property2 -> entity
          )

          Reverse.fromListUnsafe(properties) shouldBe Reverse.fromList(properties).fold(throw _, identity)
      }
    }

    "throw an exception if at least one of the properties' values is not JsonLDEntity nor an JsonLDEntityId" in {
      forAll(properties, nonEmptyList(jsonLDEntities), properties, jsonLDValues, properties, entityIds) {
        (property1, entities, property2, value, property3, entityId) =>
          val exception = intercept[Exception] {
            Reverse.fromListUnsafe(
              List(
                property1 -> JsonLD.arr(entities.toList: _*),
                property2 -> value,
                property3 -> JsonLDEntityId(entityId)
              )
            )
          }

          exception            shouldBe an[IllegalArgumentException]
          exception.getMessage shouldBe s""""@reverse" "$property2" property has to exist on an object"""
      }
    }
  }

  "equals" should {

    "return true if there are two Reverse instances with the same properties" in {
      forAll(properties, nonEmptyList(jsonLDEntities)) { (property, entities) =>
        Reverse.of(property -> entities.toList) shouldBe Reverse.of(property -> entities.toList)
      }
    }

    "return false if there are two Reverse instances with different properties" in {
      forAll(properties, nonEmptyList(jsonLDEntities)) { (property, entities) =>
        Reverse.of(property -> entities.toList) should not be Reverse.empty
      }
    }

    "return false if compared with non Reverse object" in {
      forAll(properties, nonEmptyList(jsonLDEntities)) { (property, entities) =>
        Reverse.of(property -> entities.toList) should not be 1
      }
    }
  }
}
