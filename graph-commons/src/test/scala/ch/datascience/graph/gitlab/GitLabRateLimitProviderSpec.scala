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

package ch.datascience.graph.gitlab

import cats.MonadError
import cats.implicits._
import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class GitLabRateLimitProviderSpec extends WordSpec {

  private implicit val context: MonadError[Try, Throwable] = MonadError[Try, Throwable]

  "get" should {

    "return RateLimit if defined" in {
      val rateLimit = rateLimits.generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "gitlab" -> Map(
              "rate-limit" -> rateLimit.toString
            ).asJava
          ).asJava
        ).asJava
      )

      new GitLabRateLimitProvider[Try](config).get shouldBe Success(rateLimit)
    }

    "fail if there is no 'services.gitlab.rate-limit' in the config" in {
      val config = ConfigFactory.empty

      val Failure(exception) = new GitLabRateLimitProvider[Try](config).get

      exception shouldBe a[ConfigLoadingException]
    }
  }
}
