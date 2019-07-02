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

package ch.datascience.graphservice.rdfstore

import RDFStoreGenerators._
import cats.implicits._
import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.generators.CommonGraphGenerators.basicAuthCredentials
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class RDFStoreConfigSpec extends WordSpec with ScalaCheckPropertyChecks {

  "get" should {

    "read 'services.fuseki.url', 'services.fuseki.dataset-name', 'services.fuseki.renku.username' and 'services.fuseki.renku.password' " +
      "to instantiate the RDFStoreConfig" in {
      forAll(rdfStoreConfigs) { fusekiConfig =>
        val config = ConfigFactory.parseMap(
          Map(
            "services" -> Map(
              "fuseki" -> Map(
                "url"          -> fusekiConfig.fusekiBaseUrl.toString,
                "dataset-name" -> fusekiConfig.datasetName.value,
                "renku" -> Map(
                  "username" -> fusekiConfig.authCredentials.username.value,
                  "password" -> fusekiConfig.authCredentials.password.value
                ).asJava
              ).asJava
            ).asJava
          ).asJava
        )

        val Success(actual) = RDFStoreConfig[Try](config)

        actual.fusekiBaseUrl   shouldBe fusekiConfig.fusekiBaseUrl
        actual.datasetName     shouldBe fusekiConfig.datasetName
        actual.authCredentials shouldBe fusekiConfig.authCredentials
      }
    }

    "fail if url invalid" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> "invalid-url",
              "dataset-name" -> datasetNames.generateOne.value,
              "renku" -> Map(
                "username" -> basicAuthCredentials.generateOne.username.value,
                "password" -> basicAuthCredentials.generateOne.password.value
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = RDFStoreConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if dataset-name is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> fusekiBaseUrls.generateOne.value,
              "dataset-name" -> blankStrings().generateOne,
              "renku" -> Map(
                "username" -> basicAuthCredentials.generateOne.username.value,
                "password" -> basicAuthCredentials.generateOne.password.value
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = RDFStoreConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if renku.username is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> fusekiBaseUrls.generateOne.value,
              "dataset-name" -> datasetNames.generateOne.value,
              "renku" -> Map(
                "username" -> blankStrings().generateOne,
                "password" -> basicAuthCredentials.generateOne.password.value
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = RDFStoreConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if renku.password is blank" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "url"          -> fusekiBaseUrls.generateOne.value,
              "dataset-name" -> datasetNames.generateOne.value,
              "renku" -> Map(
                "username" -> basicAuthCredentials.generateOne.username.value,
                "password" -> blankStrings().generateOne
              ).asJava
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = RDFStoreConfig[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }
  }
}
