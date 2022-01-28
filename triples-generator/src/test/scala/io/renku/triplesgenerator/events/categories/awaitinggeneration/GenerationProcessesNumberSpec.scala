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

package io.renku.triplesgenerator.events.categories.awaitinggeneration

import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import io.renku.config.ConfigLoader.ConfigLoadingException
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{nonBlankStrings, positiveInts}
import io.renku.graph.config.RenkuBaseUrlLoader
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

class GenerationProcessesNumberSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "apply" should {

    "return a GenerationProcessesNumber if there's a value for 'generation-processes-number' in the config" in {
      forAll(positiveInts()) { value =>
        val config = ConfigFactory.parseMap(
          Map("generation-processes-number" -> value.value).asJava
        )

        GenerationProcessesNumber[Try](config) shouldBe GenerationProcessesNumber(value.value).pure[Try]
      }
    }

    "fail if there's no value for the 'generation-processes-number'" in {
      val Failure(exception) = GenerationProcessesNumber[Try](ConfigFactory.empty())
      exception shouldBe an[ConfigLoadingException]
    }

    "fail if config value is invalid" in {
      val config = ConfigFactory.parseMap(
        Map("generation-processes-number" -> nonBlankStrings().generateOne.value).asJava
      )

      val Failure(exception) = RenkuBaseUrlLoader[Try](config)

      exception shouldBe an[ConfigLoadingException]
    }
  }
}
