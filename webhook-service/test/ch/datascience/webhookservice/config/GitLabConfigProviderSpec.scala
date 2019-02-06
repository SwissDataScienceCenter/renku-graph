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

package ch.datascience.webhookservice.config

import cats.MonadError
import cats.implicits._
import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import play.api.Configuration

import scala.util.{Failure, Success, Try}

class GitLabConfigProviderSpec extends WordSpec {

  private implicit val context: MonadError[Try, Throwable] = MonadError[Try, Throwable]

  "get" should {

    "return HostUrl" in {
      val gitLabUrl = validatedUrls.generateOne
      val config = Configuration.from(
        Map(
          "services" -> Map(
            "gitlab" -> Map(
              "url" -> gitLabUrl.toString()
            )
          )
        )
      )

      new GitLabConfig[Try](config).get() shouldBe Success(gitLabUrl)
    }

    "fail if there is no 'services.gitlab.url' in the config" in {
      val config = Configuration.empty

      val Failure(exception) = new GitLabConfig[Try](config).get()

      exception shouldBe a[ConfigLoadingException]
    }
  }
}
