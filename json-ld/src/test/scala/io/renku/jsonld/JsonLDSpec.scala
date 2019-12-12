/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

    "allow to construct JsonLD EntityId object" in {
      forAll { value: EntityId =>
        JsonLD.fromEntityId(value).toJson shouldBe
          json"""{
            "@id": $value
          }"""
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
  }

  "JsonLD.entity" should {

    "allow to construct JsonLD entity with multiple types" in {
      forAll(entityIds, nonEmptyList(entityTypes, minElements = 2), schemas) { (id, types, schema) =>
        val (name1, values1)            = listValueProperties(schema).generateOne
        val property1 @ (_, value1)     = name1 -> JsonLD.arr(values1.toList: _*)
        val property2 @ (name2, value2) = singleValueProperties(schema).generateOne

        JsonLD.entity(id, types, NonEmptyList.of(property1, property2)).toJson shouldBe
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

        JsonLD.entity(id, entityType, property1, property2).toJson shouldBe
          parse(s"""{
            "@id": ${id.asJson},
            "@type": ${entityType.asJson},
            "$name1": ${value1.toJson},
            "$name2": ${value2.toJson}
          }""").fold(throw _, identity)
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
}
