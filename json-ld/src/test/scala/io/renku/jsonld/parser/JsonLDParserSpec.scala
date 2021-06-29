package io.renku.jsonld.parser

import cats.syntax.all._
import io.circe.literal.JsonStringContext
import io.renku.jsonld.JsonLD.JsonLDEntity
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.Generators.{localDates, nonEmptyStrings, timestamps}
import io.renku.jsonld.generators.JsonLDGenerators._
import io.renku.jsonld.{JsonLD, Reverse}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class JsonLDParserSpec extends AnyWordSpec with should.Matchers {
  "parse" should {
    "successfully parse a json object with primitive values" in new TestCase {
      val entity = JsonLDEntity(
        entityIds.generateOne,
        entityTypesObject.generateOne,
        Map(
          schema / "boolean"   -> JsonLD.fromBoolean(Random.nextBoolean()),
          schema / "int"       -> JsonLD.fromInt(Random.nextInt()),
          schema / "long"      -> JsonLD.fromLong(Random.nextLong()),
          schema / "string"    -> JsonLD.fromString(nonEmptyStrings().generateOne),
          schema / "instant"   -> JsonLD.fromInstant(timestamps.generateOne),
          schema / "localDate" -> JsonLD.fromLocalDate(localDates.generateOne),
          schema / "arrayOfString" -> JsonLD.arr(
            nonEmptyStrings().generateNonEmptyList().map(JsonLD.fromString).toList: _*
          )
        ),
        Reverse.empty
      )
      parser.parse(entity.toJson) shouldBe Right(entity)
    }

    "successfully parse a json object with nested objects" in new TestCase {
      val entity = JsonLDEntity(
        entityIds.generateOne,
        entityTypesObject.generateOne,
        Map(
          schema / "single"          -> jsonLDEntities.generateOne,
          schema / "arrayOfEntities" -> JsonLD.arr(jsonLDEntities.generateNonEmptyList().toList: _*)
        ),
        Reverse.empty
      )
      parser.parse(entity.toJson) shouldBe Right(entity)
    }

    "successfully parse a flattened json object" in new TestCase {
      val flattenedJsonLD = JsonLDEntity(
        entityIds.generateOne,
        entityTypesObject.generateOne,
        Map(
          schema / "single"          -> jsonLDEntities.generateOne,
          schema / "arrayOfEntities" -> JsonLD.arr(jsonLDEntities.generateNonEmptyList().toList: _*)
        ),
        Reverse.empty
      ).flatten.fold(throw _, identity)

      parser.parse(flattenedJsonLD.toJson) shouldBe Right(flattenedJsonLD)
    }

    "return a ParsingFailure when a object has no @id" in new TestCase {
      val invalidJsonLD = json"""{"@type": ${entityTypes.generateOne.value} }"""
      val Left(failure) = parser.parse(invalidJsonLD)
      failure            shouldBe a[ParsingFailure]
      failure.getMessage shouldBe "Entity without @id"
    }

    "return a ParsingFailure when a object has an @id and a @value but no @type" in new TestCase {
      val invalidJsonLD =
        json"""{"@id": ${entityIds.generateOne.show} }""".deepMerge(jsonLDValues.generateOne.toJson)
      val Left(failure) = parser.parse(invalidJsonLD)
      failure            shouldBe a[ParsingFailure]
      failure.getMessage shouldBe "Entity with @id and @value but no @type"
    }

    "return a ParsingFailure when a object has an @id, a @value and a @type" in new TestCase {
      val invalidJsonLD = json"""{"@id": ${entityIds.generateOne.show}, "@type": ${entityTypes.generateOne.value}}"""
        .deepMerge(jsonLDValues.generateOne.toJson)
      val Left(failure) = parser.parse(invalidJsonLD)
      failure            shouldBe a[ParsingFailure]
      failure.getMessage shouldBe "Invalid entity"
    }

  }
  private trait TestCase {
    val schema = schemas.generateOne
    val parser = new JsonLDParser()
  }
}
