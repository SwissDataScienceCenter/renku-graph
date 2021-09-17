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

package io.renku.jsonld.parser

import cats.syntax.all._
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.Generators.{localDates, nonEmptyStrings, timestamps}
import io.renku.jsonld.generators.JsonLDGenerators._
import io.renku.jsonld.{JsonLD, Property, Reverse}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import io.renku.jsonld.syntax._

import scala.util.Random

class JsonLDParserSpec extends AnyWordSpec with should.Matchers {

  "parse" should {

    "successfully parse a json object with primitive values" in {
      val entity = JsonLD.entity(
        entityIds.generateOne,
        entityTypesObject.generateOne,
        schema / "boolean"   -> JsonLD.fromBoolean(Random.nextBoolean()),
        schema / "int"       -> JsonLD.fromInt(Random.nextInt()),
        schema / "long"      -> JsonLD.fromLong(Random.nextLong()),
        schema / "string"    -> JsonLD.fromString(nonEmptyStrings().generateOne),
        schema / "instant"   -> JsonLD.fromInstant(timestamps.generateOne),
        schema / "localDate" -> JsonLD.fromLocalDate(localDates.generateOne),
        schema / "arrayOfString" -> JsonLD.arr(
          nonEmptyStrings().generateNonEmptyList().map(JsonLD.fromString).toList: _*
        )
      )

      parser.parse(entity.toJson) shouldBe Right(entity)
    }

    "successfully parse a json object with nested objects" in {
      val entity = JsonLD.entity(
        entityIds.generateOne,
        entityTypesObject.generateOne,
        schema / "single"          -> jsonLDEntities.generateOne,
        schema / "arrayOfEntities" -> JsonLD.arr(jsonLDEntities.generateNonEmptyList().toList: _*)
      )

      parser.parse(entity.toJson) shouldBe Right(entity)
    }

    "successfully parse a flattened json object" in {
      val flattenedJsonLD = JsonLD
        .entity(
          entityIds.generateOne,
          entityTypesObject.generateOne,
          schema / "single"          -> jsonLDEntities.generateOne,
          schema / "arrayOfEntities" -> JsonLD.arr(jsonLDEntities.generateNonEmptyList().toList: _*)
        )
        .flatten
        .fold(throw _, identity)

      parser.parse(flattenedJsonLD.toJson) shouldBe Right(flattenedJsonLD)
    }

    "successfully parse an object with a reverse property" in {
      val entity = JsonLD.entity(
        entityIds.generateOne,
        entityTypesObject.generateOne,
        Reverse.ofJsonLDsUnsafe((schema / "reversedProperty") -> jsonLDEntities.generateOne),
        schema / "single" -> jsonLDEntities.generateOne
      )

      parser.parse(entity.toJson) shouldBe Right(entity)
    }

    "successfully parse an object with a single reverse property on singly entity" in {
      val nestedEntity = jsonLDEntities.generateOne
      val entity = JsonLD.entity(
        entityIds.generateOne,
        entityTypesObject.generateOne,
        Reverse.ofJsonLDsUnsafe((schema / "reversedProperty") -> nestedEntity),
        Map.empty[Property, JsonLD]
      )

      val Right(parsedJsonLD) = parser.parse(entity.flatten.fold(throw _, identity).toJson)

      parsedJsonLD.asArray.sequence.flatten should contain theSameElementsAs List(
        entity.copy(reverse = Reverse.empty),
        nestedEntity,
        JsonLD.edge(nestedEntity.id, schema / "reversedProperty", entity.id)
      )
    }

    "successfully parse an object with a single reverse property on multiple entities" in {
      val nestedEntity1 = jsonLDEntities.generateOne
      val nestedEntity2 = jsonLDEntities.generateOne
      val property      = properties.generateOne
      val entity = JsonLD.entity(
        entityIds.generateOne,
        entityTypesObject.generateOne,
        Map.empty[Property, JsonLD]
      )

      val Right(parsedJsonLD) = parser.parse(
        JsonLD
          .arr(
            nestedEntity1.copy(reverse = Reverse.fromListUnsafe(List(property -> entity.id.asJsonLD))),
            nestedEntity2.copy(reverse = Reverse.fromListUnsafe(List(property -> entity.id.asJsonLD))),
            entity
          )
          .flatten
          .fold(throw _, identity)
          .toJson
      )

      parsedJsonLD.asArray.sequence.flatten should contain theSameElementsAs List(
        entity,
        nestedEntity1,
        nestedEntity2,
        JsonLD.edge(entity.id, property, nestedEntity1.id),
        JsonLD.edge(entity.id, property, nestedEntity2.id)
      )
    }

    "successfully parse an object with a multiple reverse properties" in {
      val nestedEntity1 = jsonLDEntities.generateOne
      val nestedEntity2 = jsonLDEntities.generateOne
      val property1     = properties.generateOne
      val property2     = properties.generateOne
      val entity = JsonLD.entity(
        entityIds.generateOne,
        entityTypesObject.generateOne,
        Map.empty[Property, JsonLD]
      )

      val Right(parsedJsonLD) = parser.parse(
        JsonLD
          .arr(
            nestedEntity1.copy(reverse = Reverse.fromListUnsafe(List(property1 -> entity.id.asJsonLD))),
            nestedEntity2.copy(reverse = Reverse.fromListUnsafe(List(property2 -> entity.id.asJsonLD))),
            entity
          )
          .flatten
          .fold(throw _, identity)
          .toJson
      )

      parsedJsonLD.asArray.sequence.flatten should contain theSameElementsAs List(
        entity,
        nestedEntity1,
        nestedEntity2,
        JsonLD.edge(entity.id, property1, nestedEntity1.id),
        JsonLD.edge(entity.id, property2, nestedEntity2.id)
      )
    }

    "return a ParsingFailure when a object has no @id" in {
      val invalidJsonLD = json"""{"@type": ${entityTypes.generateOne.value}}"""

      val Left(failure) = parser.parse(invalidJsonLD)

      failure            shouldBe a[ParsingFailure]
      failure.getMessage shouldBe "Entity without @id"
    }

    "return a ParsingFailure when a object has an @id and a @value but no @type" in {
      val invalidJsonLD = json"""{
        "@id":    ${entityIds.generateOne.show},
        "@value": ${nonEmptyStrings().generateOne}
      }"""

      val Left(failure) = parser.parse(invalidJsonLD)

      failure            shouldBe a[ParsingFailure]
      failure.getMessage shouldBe "Entity with @id and @value but no @type"
    }

    "return a ParsingFailure when a object has an @id, a @value and a @type" in {
      val invalidJsonLD = json"""{
        "@id": ${entityIds.generateOne.show}, 
        "@type": ${entityTypes.generateOne.value}
      }""".deepMerge(jsonLDValues.generateOne.toJson)

      val Left(failure) = parser.parse(invalidJsonLD)

      failure            shouldBe a[ParsingFailure]
      failure.getMessage shouldBe "Invalid entity"
    }

    "return a ParsingFailure when property value is invalid" in {
      val property = properties.generateOne.show
      val invalidJsonLD = JsonLD
        .entity(
          entityIds.generateOne,
          entityTypesObject.generateOne,
          Map.empty[Property, JsonLD]
        )
        .toJson
        .deepMerge(Json.obj(property -> Json.fromString(nonEmptyStrings().generateOne)))

      val Left(failure) = parser.parse(invalidJsonLD)

      failure            shouldBe a[ParsingFailure]
      failure.getMessage shouldBe s"Malformed entity's $property property value"
    }

    "return a ParsingFailure when an array property is invalid" in {
      val property = properties.generateOne.show
      val invalidJsonLD = JsonLD
        .entity(
          entityIds.generateOne,
          entityTypesObject.generateOne,
          Map.empty[Property, JsonLD]
        )
        .toJson
        .deepMerge(Json.obj(property -> Json.arr(Json.fromString(nonEmptyStrings().generateOne))))

      val Left(failure) = parser.parse(invalidJsonLD)

      failure            shouldBe a[ParsingFailure]
      failure.getMessage shouldBe s"Malformed entity's $property array property value"
    }
  }

  private lazy val schema = schemas.generateOne
  private lazy val parser = new JsonLDParser()
}
