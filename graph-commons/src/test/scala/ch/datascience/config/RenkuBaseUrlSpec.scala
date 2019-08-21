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

package ch.datascience.config

import cats.implicits._
import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.generators.Generators._
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class RenkuBaseUrlSpec extends WordSpec with ScalaCheckPropertyChecks {

  "apply" should {

    "return a RenkuBaseUrl if there's a value for 'services.renku.url'" in {
      forAll(httpUrls) { url =>
        val config = ConfigFactory.parseMap(
          Map(
            "services" -> Map(
              "renku" -> Map(
                "url" -> url
              ).asJava
            ).asJava
          ).asJava
        )
        val Success(actual) = RenkuBaseUrl[Try](config)
      }
    }

    "fail if there's no value for the 'services.renku.url'" in {
      val Failure(exception) = RenkuBaseUrl[Try](ConfigFactory.empty())
      exception shouldBe an[ConfigLoadingException]
    }

    "fail if config value is invalid" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "renku" -> Map(
              "url" -> "abcd"
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = RenkuBaseUrl[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }
  }
}
