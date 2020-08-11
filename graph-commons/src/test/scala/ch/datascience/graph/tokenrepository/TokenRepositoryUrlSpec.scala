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

package ch.datascience.graph.tokenrepository

import cats.implicits._
import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.httpUrls
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers._
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class TokenRepositoryUrlSpec extends AnyWordSpec with should.Matchers {

  "apply" should {

    "read 'services.token-repository.url' from the config" in {
      val url = httpUrls().generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "token-repository" -> Map(
              "url" -> url
            ).asJava
          ).asJava
        ).asJava
      )

      TokenRepositoryUrl[Try](config) shouldBe Success(TokenRepositoryUrl(url))
    }

    "fail if there's no 'services.token-repository.url' entry" in {
      val config = ConfigFactory.empty()

      val Failure(exception) = TokenRepositoryUrl[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }
  }
}
