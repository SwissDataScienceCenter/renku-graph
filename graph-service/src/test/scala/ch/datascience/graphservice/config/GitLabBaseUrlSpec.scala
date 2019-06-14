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

package ch.datascience.graphservice.config

import cats.implicits._
import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.generators.Generators._
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class GitLabBaseUrlSpec extends WordSpec with ScalaCheckPropertyChecks {

  "apply" should {

    "return GitLab url if there's a valid value in the config for 'services.gitlab.url'" in {
      forAll(httpUrls) { url =>
        val config = ConfigFactory.parseMap(
          Map(
            "services" -> Map(
              "gitlab" -> Map(
                "url" -> url
              ).asJava
            ).asJava
          ).asJava
        )

        val Success(actual) = GitLabBaseUrl[Try](config)

        actual shouldBe GitLabBaseUrl(url)
      }
    }

    "fail if there's no relevant config entry" in {
      val Failure(exception) = GitLabBaseUrl[Try](ConfigFactory.empty())

      exception shouldBe an[ConfigLoadingException]
    }

    "fail if config value is invalid" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "gitlab" -> Map(
              "url" -> "abcd"
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = GitLabBaseUrl[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }
  }
}
