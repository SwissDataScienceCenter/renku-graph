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

package io.renku.graph.config

import com.typesafe.config.ConfigFactory
import io.renku.config.ConfigLoader.ConfigLoadingException
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.RenkuBaseUrl
import io.renku.graph.model.views.RdfResource
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class RenkuBaseUrlLoaderSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "apply" should {

    "return a RenkuBaseUrl if there's a value for 'services.renku.url'" in {
      forAll(httpUrls()) { url =>
        val config = ConfigFactory.parseMap(
          Map(
            "services" -> Map(
              "renku" -> Map(
                "url" -> url
              ).asJava
            ).asJava
          ).asJava
        )

        RenkuBaseUrlLoader[Try](config) shouldBe Success(RenkuBaseUrl(url))
      }
    }

    "fail if there's no value for the 'services.renku.url'" in {
      val Failure(exception) = RenkuBaseUrlLoader[Try](ConfigFactory.empty())
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

      val Failure(exception) = RenkuBaseUrlLoader[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }
  }

  "showAs[RdfResource]" should {

    "wrap the RenkuBaseUrl in <>" in {
      forAll { url: RenkuBaseUrl =>
        url.showAs[RdfResource] shouldBe s"<${url.value}>"
      }
    }
  }
}
