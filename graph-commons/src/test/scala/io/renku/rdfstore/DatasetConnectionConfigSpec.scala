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

import com.typesafe.config.ConfigFactory
import io.renku.config.ConfigLoader.ConfigLoadingException
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class DatasetConnectionConfigSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "AdminConnectionConfig.apply" should {

    "read 'services.fuseki.url', 'services.fuseki.admin.username' and 'services.fuseki.admin.password' to instantiate the AdminConnectionConfig" in {
      forAll(adminConnectionConfigs) { storeConfig =>
        val config = ConfigFactory.parseMap(
          Map(
            "services" -> Map(
              "fuseki" -> Map(
                "url" -> storeConfig.fusekiUrl.toString,
                "admin" -> Map(
                  "username" -> storeConfig.authCredentials.username.value,
                  "password" -> storeConfig.authCredentials.password.value
                ).asJava
              ).asJava
            ).asJava
          ).asJava
        )

        val Success(actual) = AdminConnectionConfig[Try](config)

        actual.fusekiUrl                shouldBe storeConfig.fusekiUrl
        actual.authCredentials.username shouldBe storeConfig.authCredentials.username
        actual.authCredentials.password shouldBe storeConfig.authCredentials.password
      }
    }

    "fail if url invalid" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url" -> "invalid-url",
              "admin" -> Map(
                "username" -> adminConnectionConfigs.generateOne.authCredentials.username.value,
                "password" -> adminConnectionConfigs.generateOne.authCredentials.password.value
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = AdminConnectionConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if username is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url" -> adminConnectionConfigs.generateOne.fusekiUrl.toString,
              "admin" -> Map(
                "username" -> "  ",
                "password" -> adminConnectionConfigs.generateOne.authCredentials.password.value
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = AdminConnectionConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if password is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url" -> adminConnectionConfigs.generateOne.fusekiUrl.toString,
              "admin" -> Map(
                "username" -> adminConnectionConfigs.generateOne.authCredentials.username.value,
                "password" -> ""
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = AdminConnectionConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }
  }

  "RenkuConnectionConfig.apply" should {

    "read 'services.fuseki.url', 'services.fuseki.renku.username' and 'services.fuseki.renku.password' to instantiate the RenkuConnectionConfig" in {
      forAll(renkuConnectionConfigs) { storeConfig =>
        val config = ConfigFactory.parseMap(
          Map(
            "services" -> Map(
              "fuseki" -> Map(
                "url" -> storeConfig.fusekiUrl.toString,
                "renku" -> Map(
                  "username" -> storeConfig.authCredentials.username.value,
                  "password" -> storeConfig.authCredentials.password.value
                ).asJava
              ).asJava
            ).asJava
          ).asJava
        )

        val Success(actual) = RenkuConnectionConfig[Try](config)

        actual.fusekiUrl                shouldBe storeConfig.fusekiUrl
        actual.datasetName              shouldBe DatasetName("renku")
        actual.authCredentials.username shouldBe storeConfig.authCredentials.username
        actual.authCredentials.password shouldBe storeConfig.authCredentials.password
      }
    }

    "fail if url invalid" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url" -> "invalid-url",
              "renku" -> Map(
                "username" -> renkuConnectionConfigs.generateOne.authCredentials.username.value,
                "password" -> renkuConnectionConfigs.generateOne.authCredentials.password.value
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = RenkuConnectionConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if username is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url" -> renkuConnectionConfigs.generateOne.fusekiUrl.toString,
              "renku" -> Map(
                "username" -> "  ",
                "password" -> renkuConnectionConfigs.generateOne.authCredentials.password.value
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = RenkuConnectionConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if password is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url" -> renkuConnectionConfigs.generateOne.fusekiUrl.toString,
              "renku" -> Map(
                "username" -> renkuConnectionConfigs.generateOne.authCredentials.username.value,
                "password" -> ""
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = RenkuConnectionConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }
  }

  "MigrationsConnectionConfig.apply" should {

    "read 'services.fuseki.url', 'services.fuseki.admin.username' and 'services.fuseki.admin.password' to instantiate the RenkuConnectionConfig" in {
      forAll(renkuConnectionConfigs) { storeConfig =>
        val config = ConfigFactory.parseMap(
          Map(
            "services" -> Map(
              "fuseki" -> Map(
                "url" -> storeConfig.fusekiUrl.toString,
                "admin" -> Map(
                  "username" -> storeConfig.authCredentials.username.value,
                  "password" -> storeConfig.authCredentials.password.value
                ).asJava
              ).asJava
            ).asJava
          ).asJava
        )

        val Success(actual) = MigrationsConnectionConfig[Try](config)

        actual.fusekiUrl                shouldBe storeConfig.fusekiUrl
        actual.datasetName              shouldBe DatasetName("migrations")
        actual.authCredentials.username shouldBe storeConfig.authCredentials.username
        actual.authCredentials.password shouldBe storeConfig.authCredentials.password
      }
    }
  }
}
