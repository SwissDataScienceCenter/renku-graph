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
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class FusekiUserConfigSpec extends WordSpec with ScalaCheckPropertyChecks {

  "apply" should {

    "read 'services.fuseki.url', 'services.fuseki.dataset-name', 'services.fuseki.renkuuser.username' and 'services.fuseki.renkuuser.password' to instantiate the FusekiUserConfig" in {
      forAll(fusekiUserConfigs) { userConfig =>
        val config = ConfigFactory.parseMap(
          Map(
            "services" -> Map(
              "fuseki" -> Map(
                "url"          -> userConfig.fusekiBaseUrl.toString,
                "dataset-name" -> userConfig.datasetName.value,
                "renkuuser" -> Map(
                  "username" -> userConfig.authCredentials.username.value,
                  "password" -> userConfig.authCredentials.password.value
                ).asJava
              ).asJava
            ).asJava
          ).asJava
        )

        val Success(actual) = FusekiUserConfig[Try](config)

        actual.fusekiBaseUrl            shouldBe userConfig.fusekiBaseUrl
        actual.datasetName              shouldBe userConfig.datasetName
        actual.authCredentials.username shouldBe userConfig.authCredentials.username
        actual.authCredentials.password shouldBe userConfig.authCredentials.password
      }
    }

    "fail if url invalid" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> "invalid-url",
              "dataset-name" -> fusekiUserConfigs.generateOne.datasetName.value,
              "renkuuser" -> Map(
                "username" -> fusekiUserConfigs.generateOne.authCredentials.username.value,
                "password" -> fusekiUserConfigs.generateOne.authCredentials.password.value
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = FusekiUserConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if dataset-name is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> fusekiUserConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> "  ",
              "renkuuser" -> Map(
                "username" -> fusekiUserConfigs.generateOne.authCredentials.username.value,
                "password" -> fusekiUserConfigs.generateOne.authCredentials.password.value
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = FusekiUserConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if username is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> fusekiUserConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> fusekiUserConfigs.generateOne.datasetName.value,
              "renkuuser" -> Map(
                "username" -> "  ",
                "password" -> fusekiUserConfigs.generateOne.authCredentials.password.value
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = FusekiUserConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if password is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> fusekiUserConfigs.generateOne.fusekiBaseUrl.toString,
              "dataset-name" -> fusekiUserConfigs.generateOne.datasetName.value,
              "admin" -> Map(
                "username" -> fusekiUserConfigs.generateOne.authCredentials.username.value,
                "password" -> ""
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = FusekiUserConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }
  }
}
