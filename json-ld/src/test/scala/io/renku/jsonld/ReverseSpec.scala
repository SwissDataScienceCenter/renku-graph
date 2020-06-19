package io.renku.jsonld

import cats.data.NonEmptyList
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.renku.jsonld.JsonLD.JsonLDEntity
import io.renku.jsonld.generators.Generators
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.JsonLDGenerators._
import io.renku.jsonld.syntax._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ReverseSpec extends WordSpec with ScalaCheckPropertyChecks {
  "fromList" should {
    "return right if all the items are JsonLDEntities" in {
      forAll(Generators.nonEmptyList(jsonLDEntities), properties) {
        (jsonLDEntities: NonEmptyList[JsonLD], property: Property) =>
          val Right(entity) = Reverse.fromList(property -> jsonLDEntities.toList)

          entity.asJson shouldBe json"""{
          ${property.url}:${Json.arr(jsonLDEntities.map(_.toJson).toList: _*)}
        }"""

      }
    }
  }
}
