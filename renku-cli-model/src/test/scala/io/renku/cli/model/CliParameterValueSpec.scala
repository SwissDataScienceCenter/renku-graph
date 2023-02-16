/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.cli.model

import io.circe.syntax._
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.ParameterValueGenerators
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CliParameterValueSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with JsonLDCodecMatchers {

  val parameterValueGen = ParameterValueGenerators.parameterValueGen

  "decode/encode" should {
    "be compatible" in {
      forAll(parameterValueGen) { cliParameterValue =>
        assertCompatibleCodec(cliParameterValue)
      }
    }

    "work on multiple items" in {
      forAll(parameterValueGen, parameterValueGen) { (cliParam1, cliParam2) =>
        assertCompatibleCodec(cliParam1, cliParam2)
      }
    }
  }

  "asString" should {
    "return the plain value as a string " in {
      List("blabla".asJson -> "blabla", 124.asJson -> 124, true.asJson -> true, 12.04.asJson -> 12.04).foreach {
        case (jsonValue, plainValue) =>
          val jsonLDString =
            s"""
               |  {
               |    "@id": "https://renku-kg-dev.dev.renku.ch/activities/2b254b7ac7f84eca9cfb7efbe50bfe13/parameter-value/4b26542029cc4bdcadb28ffd9a5c5b13",
               |    "@type": [
               |      "http://schema.org/PropertyValue",
               |      "https://swissdatasciencecenter.github.io/renku-ontology#ParameterValue"
               |    ],
               |    "http://schema.org/value": [
               |      {
               |        "@value": $jsonValue
               |      }
               |    ],
               |    "http://schema.org/valueReference": [
               |      {
               |        "@id": "https://renku-kg-dev.dev.renku.ch/plans/7410c66856dd4b63ba2811b0ec473056/inputs/2"
               |      }
               |    ]
               |  }
               |""".stripMargin

          val result = io.renku.jsonld.parser
            .parse(jsonLDString)
            .flatMap(_.cursor.as[CliParameterValue])
            .fold(throw _, identity)
          result.value.asString shouldBe plainValue.toString
      }
    }
  }
}
