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

package ch.datascience.triplesgenerator.config

import cats.implicits._
import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators._
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.prop.PropertyChecks

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class FusekiConfigProviderSpec extends WordSpec with PropertyChecks {

  "get" should {

    "read 'services.fuseki.url', 'services.fuseki.dataset-name', 'services.fuseki.dataset-type', 'services.fuseki.username' and 'services.fuseki.password' to instantiate the FusekiConfig" in {
      forAll(fusekiConfigs) { fusekiConfig =>
        val config = ConfigFactory.parseMap(
          Map(
            "services" -> Map(
              "fuseki" -> Map(
                "url"          -> fusekiConfig.fusekiBaseUrl.toString,
                "dataset-name" -> fusekiConfig.datasetName.value,
                "dataset-type" -> fusekiConfig.datasetType.value,
                "username"     -> fusekiConfig.username.value,
                "password"     -> fusekiConfig.password.value
              ).asJava
            ).asJava
          ).asJava
        )

        val Success(actual) = new FusekiConfigProvider[Try](config).get

        actual.fusekiBaseUrl shouldBe fusekiConfig.fusekiBaseUrl
        actual.datasetName   shouldBe fusekiConfig.datasetName
        actual.datasetType   shouldBe fusekiConfig.datasetType
        actual.username      shouldBe fusekiConfig.username
        actual.password      shouldBe fusekiConfig.password
      }
    }

    "fail if url invalid" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> "invalid-url",
              "dataset-name" -> fusekiConfigs.generateOne.datasetName.value,
              "dataset-type" -> fusekiConfigs.generateOne.datasetType.value,
              "username"     -> fusekiConfigs.generateOne.datasetName.value,
              "password"     -> fusekiConfigs.generateOne.password.value
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = new FusekiConfigProvider[Try](config).get

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if dataset-name is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> fusekiConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> "  ",
              "dataset-type" -> fusekiConfigs.generateOne.datasetType.value,
              "username"     -> fusekiConfigs.generateOne.username.value,
              "password"     -> fusekiConfigs.generateOne.password.value
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = new FusekiConfigProvider[Try](config).get

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if dataset-type is unknown" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> fusekiConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> fusekiConfigs.generateOne.datasetName.value,
              "dataset-type" -> "unknown",
              "username"     -> fusekiConfigs.generateOne.username.value,
              "password"     -> fusekiConfigs.generateOne.password.value
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = new FusekiConfigProvider[Try](config).get

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if username is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> fusekiConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> fusekiConfigs.generateOne.datasetName.value,
              "dataset-type" -> fusekiConfigs.generateOne.datasetType.value,
              "username"     -> "  ",
              "password"     -> fusekiConfigs.generateOne.password.value
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = new FusekiConfigProvider[Try](config).get

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if password is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> fusekiConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> fusekiConfigs.generateOne.datasetName.value,
              "dataset-type" -> fusekiConfigs.generateOne.datasetType.value,
              "username"     -> fusekiConfigs.generateOne.username.value,
              "password"     -> ""
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = new FusekiConfigProvider[Try](config).get

      exception shouldBe an[ConfigLoadingException]
    }
  }
}
