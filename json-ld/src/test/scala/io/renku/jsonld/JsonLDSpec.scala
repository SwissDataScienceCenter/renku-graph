package io.renku.jsonld

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
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class JsonLDSpec extends WordSpec with ScalaCheckPropertyChecks {

  "JsonLD.fromString" should {

    "allow to construct JsonLD String value" in {
      forAll { value: String =>
        JsonLD.fromString(value).toJson shouldBe
          json"""{
            "@value": $value
          }"""
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
  }

  "JsonLD.fromEntityId" should {

    "allow to construct JsonLD Long value" in {
      forAll { value: EntityId =>
        JsonLD.fromEntityId(value).toJson shouldBe
          json"""{
            "@id": $value
          }"""
      }
    }
  }

  "JsonLD.entity" should {

    "allow to construct JsonLD entity" in {
      forAll(entityIds, nonEmptyList(entityTypes, maxElements = 3), schemas) { (id, types, schema) =>
        val property1 @ (name1, value1) = propertiesGen(schema).generateOne
        val property2 @ (name2, value2) = propertiesGen(schema).generateOne

        JsonLD.entity(id, types, NonEmptyList.of(property1, property2)).toJson shouldBe
          parse(s"""{
            "@id": ${id.asJson},
            "@type": ${Json.arr(types.toList.map(_.asJson): _*)},
            "$name1": [${value1.toJson}],
            "$name2": [${value2.toJson}]
          }""").fold(throw _, identity)
      }
    }
  }

  private def propertiesGen(schema: Schema): Gen[(Property, JsonLD)] =
    for {
      property <- nonBlankStrings() map (p => schema / p.value)
      value    <- nonBlankStrings() map (v => JsonLD.fromString(v.value))
    } yield property -> value
}
