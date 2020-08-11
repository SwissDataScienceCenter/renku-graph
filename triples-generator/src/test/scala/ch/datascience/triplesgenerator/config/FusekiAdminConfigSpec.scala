/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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
import org.scalatest.matchers._
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class FusekiAdminConfigSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "apply" should {

    "read 'services.fuseki.url', 'services.fuseki.dataset-name', 'services.fuseki.dataset-type', 'services.fuseki.admin.username' and 'services.fuseki.admin.password' to instantiate the FusekiAdminConfig" in {
      forAll(fusekiAdminConfigs) { adminConfig =>
        val config = ConfigFactory.parseMap(
          Map(
            "services" -> Map(
              "fuseki" -> Map(
                "url"          -> adminConfig.fusekiBaseUrl.toString,
                "dataset-name" -> adminConfig.datasetName.value,
                "dataset-type" -> adminConfig.datasetType.value,
                "admin" -> Map(
                  "username" -> adminConfig.authCredentials.username.value,
                  "password" -> adminConfig.authCredentials.password.value
                ).asJava
              ).asJava
            ).asJava
          ).asJava
        )

        val Success(actual) = FusekiAdminConfig[Try](config)

        actual.fusekiBaseUrl            shouldBe adminConfig.fusekiBaseUrl
        actual.datasetName              shouldBe adminConfig.datasetName
        actual.datasetType              shouldBe adminConfig.datasetType
        actual.authCredentials.username shouldBe adminConfig.authCredentials.username
        actual.authCredentials.password shouldBe adminConfig.authCredentials.password
      }
    }

    "fail if url invalid" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> "invalid-url",
              "dataset-name" -> fusekiAdminConfigs.generateOne.datasetName.value,
              "dataset-type" -> fusekiAdminConfigs.generateOne.datasetType.value,
              "admin" -> Map(
                "username" -> fusekiAdminConfigs.generateOne.authCredentials.username.value,
                "password" -> fusekiAdminConfigs.generateOne.authCredentials.password.value
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = FusekiAdminConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if dataset-name is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> fusekiAdminConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> "  ",
              "dataset-type" -> fusekiAdminConfigs.generateOne.datasetType.value,
              "admin" -> Map(
                "username" -> fusekiAdminConfigs.generateOne.authCredentials.username.value,
                "password" -> fusekiAdminConfigs.generateOne.authCredentials.password.value
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = FusekiAdminConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if dataset-type is unknown" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> fusekiAdminConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> fusekiAdminConfigs.generateOne.datasetName.value,
              "dataset-type" -> "unknown",
              "admin" -> Map(
                "username" -> fusekiAdminConfigs.generateOne.authCredentials.username.value,
                "password" -> fusekiAdminConfigs.generateOne.authCredentials.password.value
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = FusekiAdminConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if username is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> fusekiAdminConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> fusekiAdminConfigs.generateOne.datasetName.value,
              "dataset-type" -> fusekiAdminConfigs.generateOne.datasetType.value,
              "admin" -> Map(
                "username" -> "  ",
                "password" -> fusekiAdminConfigs.generateOne.authCredentials.password.value
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = FusekiAdminConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if password is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> fusekiAdminConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> fusekiAdminConfigs.generateOne.datasetName.value,
              "dataset-type" -> fusekiAdminConfigs.generateOne.datasetType.value,
              "admin" -> Map(
                "username" -> fusekiAdminConfigs.generateOne.authCredentials.username.value,
                "password" -> ""
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = FusekiAdminConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }
  }
}
