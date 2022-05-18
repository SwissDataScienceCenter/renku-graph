/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.rdfstore

import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import cats.syntax.all._
import io.circe.Decoder
import io.circe.literal._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._

class ListResultsDecoderSpec extends AnyWordSpec with should.Matchers with ResultsDecoding {

  "apply" should {

    "return a circe Decoder for List of items of the defined type that work with the SparQL query response" in new {
      val decoder: Decoder[List[(String, Int)]] = ListResultsDecoder[(String, Int)] { implicit cursor =>
        (extract[String]("stringProp") -> extract[Int]("intProp")).mapN(_ -> _)
      }

      val stringPropValue1 = nonEmptyStrings().generateOne
      val intPropValue1    = positiveInts().generateOne.value
      val stringPropValue2 = nonEmptyStrings().generateOne
      val intPropValue2    = positiveInts().generateOne.value

      val json = json"""{
        "results": {
          "bindings": [
            {
              "stringProp": { "type": "literal" , "value": $stringPropValue1 },
              "intProp": { "type": "literal" , "value": $intPropValue1 }
            },
            {
              "stringProp": { "type": "literal" , "value": $stringPropValue2 },
              "intProp": { "type": "literal" , "value": $intPropValue2 }
            }
          ]
        }      
      }"""

      json.as(decoder) shouldBe List(stringPropValue1 -> intPropValue1, stringPropValue2 -> intPropValue2).asRight
    }
  }
}

class OptionalResultDecoderSpec extends AnyWordSpec with should.Matchers with ResultsDecoding {

  "apply" should {
    val error = sentences().generateOne.value
    val decoder: Decoder[Option[(String, Int)]] = OptionalResultDecoder[(String, Int)](onMultiple = error) {
      implicit cursor => (extract[String]("stringProp") -> extract[Int]("intProp")).mapN(_ -> _)
    }

    "return a circe Decoder for an Optional object of the defined type that work with the SparQL query response " +
      "- case when a single valid object exists in the response" in new {

        val stringPropValue = nonEmptyStrings().generateOne
        val intPropValue    = positiveInts().generateOne.value

        val json = json"""{
          "results": {
            "bindings": [
              {
                "stringProp": { "type": "literal" , "value": $stringPropValue },
                "intProp": { "type": "literal" , "value": $intPropValue }
              }
            ]
          }      
        }"""

        json.as(decoder) shouldBe Some(stringPropValue -> intPropValue).asRight
      }

    "return a circe Decoder for an Optional object of the defined type that work with the SparQL query response " +
      "- case with an empty response" in new {

        val json = json"""{
          "results": {
            "bindings": []
          }      
        }"""

        json.as(decoder) shouldBe None.asRight
      }

    "return a circe Decoder for an Optional object of the defined type that work with the SparQL query response " +
      "- case multiple results in the response" in new {

        val json = json"""{
          "results": {
            "bindings": [
              {
                "stringProp": { "type": "literal" , "value": ${nonEmptyStrings().generateOne} },
                "intProp": { "type": "literal" , "value": ${positiveInts().generateOne.value} }
              },
              {
                "stringProp": { "type": "literal" , "value": ${nonEmptyStrings().generateOne} },
                "intProp": { "type": "literal" , "value": ${positiveInts().generateOne.value} }
              }
            ]
          }      
        }"""

        val Left(failure) = json.as(decoder)

        failure.message shouldBe error
      }
  }
}

class UniqueResultDecoderSpec extends AnyWordSpec with should.Matchers with ResultsDecoding {

  "apply" should {
    val onEmptyError    = sentences().generateOne.value
    val onMultipleError = sentences().generateOne.value
    val decoder: Decoder[(String, Int)] =
      UniqueResultDecoder[(String, Int)](onEmpty = onEmptyError, onMultiple = onMultipleError) { implicit cursor =>
        (extract[String]("stringProp") -> extract[Int]("intProp")).mapN(_ -> _)
      }

    "return a circe Decoder for an Optional object of the defined type that work with the SparQL query response " +
      "- case when a single valid object exists in the response" in new {

        val stringPropValue = nonEmptyStrings().generateOne
        val intPropValue    = positiveInts().generateOne.value

        val json = json"""{
          "results": {
            "bindings": [
              {
                "stringProp": { "type": "literal" , "value": $stringPropValue },
                "intProp": { "type": "literal" , "value": $intPropValue }
              }
            ]
          }      
        }"""

        json.as(decoder) shouldBe (stringPropValue -> intPropValue).asRight
      }

    "return a circe Decoder for an Optional object of the defined type that work with the SparQL query response " +
      "- case with an empty response" in new {

        val json = json"""{
          "results": {
            "bindings": []
          }      
        }"""

        val Left(failure) = json.as(decoder)

        failure.message shouldBe onEmptyError
      }

    "return a circe Decoder for an Optional object of the defined type that work with the SparQL query response " +
      "- case multiple results in the response" in new {

        val json = json"""{
          "results": {
            "bindings": [
              {
                "stringProp": { "type": "literal" , "value": ${nonEmptyStrings().generateOne} },
                "intProp": { "type": "literal" , "value": ${positiveInts().generateOne.value} }
              },
              {
                "stringProp": { "type": "literal" , "value": ${nonEmptyStrings().generateOne} },
                "intProp": { "type": "literal" , "value": ${positiveInts().generateOne.value} }
              }
            ]
          }      
        }"""

        val Left(failure) = json.as(decoder)

        failure.message shouldBe onMultipleError
      }
  }
}
