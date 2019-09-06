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

package ch.datascience.webhookservice.project

import cats.implicits._
import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.webhookservice.generators.WebhookServiceGenerators._
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class SelfUrlSpec extends WordSpec {

  "apply" should {

    "read 'services.self.url' from the config" in {
      val selfUrl = selfUrls.generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "self" -> Map(
              "url" -> selfUrl.toString()
            ).asJava
          ).asJava
        ).asJava
      )

      SelfUrl[Try](config) shouldBe Success(selfUrl)
    }

    "fail if the value for 'services.self.url' is invalid" in {
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "self" -> Map(
              "url" -> "123"
            ).asJava
          ).asJava
        ).asJava
      )

      val Failure(exception) = SelfUrl[Try](config)

      exception.getMessage should startWith(
        "Cannot convert '123'"
      )
    }

    "fail if there's no 'services.self.url' entry" in {
      val Failure(exception) = SelfUrl[Try](ConfigFactory.empty())

      exception shouldBe an[ConfigLoadingException]
    }
  }
}
