/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.config

import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.httpUrls
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import ch.datascience.generators.CommonGraphGenerators._

class GitLabUrlSpec extends AnyWordSpec with should.Matchers {

  "apply" should {

    "read 'services.gitlab.url' from the config" in {
      val url = httpUrls().generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "gitlab" -> Map(
              "url" -> url
            ).asJava
          ).asJava
        ).asJava
      )

      GitLabUrl[Try](config) shouldBe Success(GitLabUrl(url))
    }

    "fail if there's no 'services.gitlab.url' entry" in {
      val Failure(exception) = GitLabUrl[Try](ConfigFactory.empty())

      exception shouldBe an[ConfigLoadingException]
    }
  }

  "apiV4" should {
    "return the full URL including the API path" in {

      val url = gitLabUrls.generateOne

      url.apiV4.value shouldBe s"$url/api/v4"
      url.apiV4       shouldBe a[GitLabApiUrl]
    }
  }

}
