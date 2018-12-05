/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

package ch.datascience.config

import java.net.MalformedURLException

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.prop.PropertyChecks
import play.api.Configuration

class FusekiConfigSpec extends WordSpec with PropertyChecks {

  "apply" should {

    "read 'services.fuseki.url', 'services.fuseki.dataset-name', 'services.fuseki.dataset-type', 'services.fuseki.username' and 'services.fuseki.password' to instantiate the FusekiConfig" in {
      forAll( fusekiConfigs ) { fusekiConfig =>
        val config = Configuration.from(
          Map( "services" ->
            Map(
              "fuseki" -> Map(
                "url" -> fusekiConfig.fusekiBaseUrl.toString,
                "dataset-name" -> fusekiConfig.datasetName.value,
                "dataset-type" -> fusekiConfig.datasetType.value,
                "username" -> fusekiConfig.username.value,
                "password" -> fusekiConfig.password.value
              )
            ) )
        )

        val actual = new FusekiConfig( config )

        actual.fusekiBaseUrl shouldBe fusekiConfig.fusekiBaseUrl
        actual.datasetName shouldBe fusekiConfig.datasetName
        actual.datasetType shouldBe fusekiConfig.datasetType
        actual.username shouldBe fusekiConfig.username
        actual.password shouldBe fusekiConfig.password
      }
    }

    "throw an IllegalArgumentException if url invalid" in {
      val config = Configuration.from(
        Map( "services" ->
          Map(
            "fuseki" -> Map(
              "url" -> "invalid-url",
              "dataset-name" -> fusekiConfigs.generateOne.datasetName.value,
              "dataset-type" -> fusekiConfigs.generateOne.datasetType.value,
              "username" -> fusekiConfigs.generateOne.datasetName.value,
              "password" -> fusekiConfigs.generateOne.password.value
            )
          ) )
      )

      an[MalformedURLException] should be thrownBy new FusekiConfig( config )
    }

    "throw an IllegalArgumentException if dataset-name is blank" in {
      val config = Configuration.from(
        Map( "services" ->
          Map(
            "fuseki" -> Map(
              "url" -> fusekiConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> "  ",
              "dataset-type" -> fusekiConfigs.generateOne.datasetType.value,
              "username" -> fusekiConfigs.generateOne.username.value,
              "password" -> fusekiConfigs.generateOne.password.value
            )
          ) )
      )

      an[IllegalArgumentException] should be thrownBy new FusekiConfig( config )
    }

    "throw an IllegalArgumentException if dataset-type is unknown" in {
      val config = Configuration.from(
        Map( "services" ->
          Map(
            "fuseki" -> Map(
              "url" -> fusekiConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> fusekiConfigs.generateOne.datasetName.value,
              "dataset-type" -> "unknown",
              "username" -> fusekiConfigs.generateOne.username.value,
              "password" -> fusekiConfigs.generateOne.password.value
            )
          ) )
      )

      an[IllegalArgumentException] should be thrownBy new FusekiConfig( config )
    }

    "throw an IllegalArgumentException if username is blank" in {
      val config = Configuration.from(
        Map( "services" ->
          Map(
            "fuseki" -> Map(
              "url" -> fusekiConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> fusekiConfigs.generateOne.datasetName.value,
              "dataset-type" -> fusekiConfigs.generateOne.datasetType.value,
              "username" -> "  ",
              "password" -> fusekiConfigs.generateOne.password.value
            )
          ) )
      )

      an[IllegalArgumentException] should be thrownBy new FusekiConfig( config )
    }

    "throw an IllegalArgumentException if password is blank" in {
      val config = Configuration.from(
        Map( "services" ->
          Map(
            "fuseki" -> Map(
              "url" -> fusekiConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> fusekiConfigs.generateOne.datasetName.value,
              "dataset-type" -> fusekiConfigs.generateOne.datasetType.value,
              "username" -> fusekiConfigs.generateOne.username.value,
              "password" -> ""
            )
          ) )
      )

      an[IllegalArgumentException] should be thrownBy new FusekiConfig( config )
    }
  }
}
