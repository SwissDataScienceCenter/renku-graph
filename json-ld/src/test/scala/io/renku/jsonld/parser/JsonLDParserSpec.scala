package io.renku.jsonld.parser

import cats.syntax.all._
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.Generators.{localDates, nonEmptyStrings, timestamps}
import io.renku.jsonld.generators.JsonLDGenerators._
import io.renku.jsonld.{JsonLD, Property}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

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
