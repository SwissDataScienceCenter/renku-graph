package io.renku.jsonld

import cats.data.NonEmptyList
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.parser._
import io.renku.jsonld.generators.Generators.Implicits._
import io.renku.jsonld.generators.Generators._
import io.renku.jsonld.generators.JsonLDGenerators._
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class JsonLDSpec extends WordSpec with ScalaCheckPropertyChecks {

  "JsonLD.entity" should {

    "allow to construct JsonLD object" in {
      forAll(entityIds, nonEmptyList(entityTypes, maxElements = 3), schemas) { (id, types, schema) =>
        val property1 @ (name1, value1) = propertiesGen(schema).generateOne
        val property2 @ (name2, value2) = propertiesGen(schema).generateOne

        JsonLD.entity(id, types, NonEmptyList.of(property1, property2)).toJson shouldBe
          parse(s"""{
            "@id": ${id.toJson},
            "@type": ${Json.arr(types.toList.map(_.toJson): _*)},
            "$name1": [{
              "@value": "${value1.toJson}"
            }],
            "$name2": [{
              "@value": "${value2.toJson}"
            }]
          }""")
      }
    }
  }

  private def propertiesGen(schema: Schema): Gen[(Property, JsonLD)] =
    for {
      property <- nonBlankStrings() map (p => schema / p.value)
      value    <- nonBlankStrings() map (v => JsonLD.fromString(v.value))
    } yield property -> value
}
