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

package io.renku.graph.config

import com.typesafe.config.ConfigFactory
import io.renku.config.ConfigLoader.ConfigLoadingException
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.httpUrls
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.{GitLabApiUrl, GitLabUrl}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class GitLabUrlLoaderSpec extends AnyWordSpec with should.Matchers {

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

      GitLabUrlLoader[Try](config) shouldBe Success(GitLabUrl(url))
    }

    "fail if there's no 'services.gitlab.url' entry" in {
      val Failure(exception) = GitLabUrlLoader[Try](ConfigFactory.empty())

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
