/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.api

import com.typesafe.config.ConfigFactory
import io.renku.config.ConfigLoader.ConfigLoadingException
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.httpUrls
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.Try

class TokenRepositoryUrlSpec extends AnyWordSpec with should.Matchers with TryValues {

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

      TokenRepositoryUrl[Try](config).success.value shouldBe TokenRepositoryUrl(url)
    }

    "fail if there's no 'services.token-repository.url' entry" in {
      TokenRepositoryUrl[Try](ConfigFactory.empty()).failure.exception shouldBe an[ConfigLoadingException]
    }
  }
}
