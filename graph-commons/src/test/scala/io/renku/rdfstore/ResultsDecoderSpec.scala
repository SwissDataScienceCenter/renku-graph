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

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.Decoder
import io.circe.literal._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ResultsDecoderSpec extends AnyWordSpec with should.Matchers with ResultsDecoder {

  "apply for List of Objects" should {

    val decoder: Decoder[List[(String, Int)]] = ResultsDecoder[List, (String, Int)] { implicit cursor =>
      (extract[String]("stringProp") -> extract[Int]("intProp")).mapN(_ -> _)
    }

    "return a circe Decoder for a List of Objects of the defined type that work with the SparQL query response" in new {

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

  "apply for NonEmptyList of Objects" should {

    val decoder: Decoder[NonEmptyList[(String, Int)]] = ResultsDecoder[NonEmptyList, (String, Int)] { implicit cursor =>
      (extract[String]("stringProp") -> extract[Int]("intProp")).mapN(_ -> _)
    }

    "return a circe Decoder for a NonEmptyList of Objects of the defined type that work with the SparQL query response" in new {

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

      json.as(decoder) shouldBe NonEmptyList
        .of(stringPropValue1 -> intPropValue1, stringPropValue2 -> intPropValue2)
        .asRight
    }

    "return a circe Decoder for a NonEmptyList of Objects " +
      "- case with an empty response" in new {

        val json = json"""{
          "results": {
            "bindings": []
          }      
        }"""

        json.as(decoder).leftMap(_.message) shouldBe "No records found but expected at least one".asLeft
      }

    "return a circe Decoder for a NonEmptyList of Objects " +
      "- case with an empty response and a custom error" in new {

        val json = json"""{
          "results": {
            "bindings": []
          }      
        }"""

        val error = nonEmptyStrings().generateOne
        val decoder: Decoder[NonEmptyList[(String, Int)]] = ResultsDecoder[NonEmptyList, (String, Int)] {
          implicit cursor =>
            (extract[String]("stringProp") -> extract[Int]("intProp")).mapN(_ -> _)
        }(toNonEmptyList(error))

        json.as(decoder).leftMap(_.message) shouldBe error.asLeft
      }
  }

  "apply for Option of Objects" should {

    val decoder: Decoder[Option[(String, Int)]] = ResultsDecoder[Option, (String, Int)] { implicit cursor =>
      (extract[String]("stringProp") -> extract[Int]("intProp")).mapN(_ -> _)
    }

    "return a circe Decoder for an Option of Object of the defined type that work with the SparQL query response " +
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

    "return a circe Decoder for an Option of Object " +
      "- case with an empty response" in new {

        val json = json"""{
          "results": {
            "bindings": []
          }      
        }"""

        json.as(decoder) shouldBe None.asRight
      }

    "return a circe Decoder for an Option of Object " +
      "- case with multiple results" in new {

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

        json.as(decoder).leftMap(_.message) shouldBe "Multiple records found but expected one or zero".asLeft
      }

    "return a circe Decoder for an Option of Object " +
      "- case with multiple results and a custom message" in new {

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

        val decoderWithErrorFunction: Decoder[Option[(String, Int)]] = ResultsDecoder[Option, (String, Int)] {
          implicit cursor =>
            (extract[String]("stringProp") -> extract[Int]("intProp")).mapN(_ -> _)
        }(toOption((list: List[(String, Int)]) => s"Found ${list.size}"))

        json.as(decoderWithErrorFunction).leftMap(_.message) shouldBe "Found 2".asLeft

        val error = nonEmptyStrings().generateOne
        val decoderWithFixedError: Decoder[Option[(String, Int)]] = ResultsDecoder[Option, (String, Int)] {
          implicit cursor =>
            (extract[String]("stringProp") -> extract[Int]("intProp")).mapN(_ -> _)
        }(toOption(error))

        json.as(decoderWithFixedError).leftMap(_.message) shouldBe error.asLeft
      }
  }

  "apply for Single of Object" should {

    val decoder: Decoder[(String, Int)] = ResultsDecoder.single[(String, Int)] { implicit cursor =>
      (extract[String]("stringProp") -> extract[Int]("intProp")).mapN(_ -> _)
    }

    "return a circe Decoder for a single Object of the defined type that work with the SparQL query response " +
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

    "return a circe Decoder for a single result " +
      "- case with an empty response" in new {

        val json = json"""{
          "results": {
            "bindings": []
          }      
        }"""

        json.as(decoder).leftMap(_.message) shouldBe "No records found but expected exactly one".asLeft
      }

    "return a circe Decoder for a single result " +
      "- case with multiple results" in new {

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

        json.as(decoder).leftMap(_.message) shouldBe "Multiple records found but expected exactly one".asLeft
      }

    "return a circe Decoder for a single result " +
      "- case with multiple results and a custom message" in new {

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

        val errorOnEmpty = nonEmptyStrings().generateOne
        val decoderWithErrorFunction: Decoder[(String, Int)] = ResultsDecoder.single[(String, Int)] { implicit cursor =>
          (extract[String]("stringProp") -> extract[Int]("intProp")).mapN(_ -> _)
        }(toSingle(errorOnEmpty, onMultiple = (list: List[(String, Int)]) => s"Found ${list.size}"))

        json.as(decoderWithErrorFunction).leftMap(_.message) shouldBe "Found 2".asLeft

        val errorOnMultiple = nonEmptyStrings().generateOne
        val decoderWithFixedErrors: Decoder[(String, Int)] = ResultsDecoder.single[(String, Int)] { implicit cursor =>
          (extract[String]("stringProp") -> extract[Int]("intProp")).mapN(_ -> _)
        }(toSingle(errorOnEmpty, errorOnMultiple))

        json.as(decoderWithFixedErrors).leftMap(_.message) shouldBe errorOnMultiple.asLeft

        val decoderWithErrors: Decoder[(String, Int)] =
          ResultsDecoder.singleWithErrors(errorOnEmpty, errorOnMultiple) { implicit cursor =>
            (extract[String]("stringProp") -> extract[Int]("intProp")).mapN(_ -> _)
          }

        json.as(decoderWithErrors).leftMap(_.message) shouldBe errorOnMultiple.asLeft
      }
  }
}
