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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.triplesgenerated

import com.typesafe.config.ConfigFactory
import io.renku.config.ConfigLoader.ConfigLoadingException
import io.renku.generators.Generators.{nonBlankStrings, positiveInts}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.config.RenkuUrlLoader
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.TryValues
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.jdk.CollectionConverters._
import scala.util.Try

class ConcurrentProcessesNumberSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with TryValues {

  "apply" should {

    "return a GenerationProcessesNumber if there's a value for 'transformation-processes-number' in the config" in {
      forAll(positiveInts()) { value =>
        val config = ConfigFactory.parseMap(
          Map("transformation-processes-number" -> value.value).asJava
        )

        ConcurrentProcessesNumber[Try](config).success.value shouldBe ConcurrentProcessesNumber(value.value)
      }
    }

    "fail if there's no value for the 'transformation-processes-number'" in {
      ConcurrentProcessesNumber[Try](ConfigFactory.empty()).failure.exception shouldBe an[ConfigLoadingException]
    }

    "fail if config value is invalid" in {
      val config = ConfigFactory.parseMap(
        Map("transformation-processes-number" -> nonBlankStrings().generateOne.value).asJava
      )

      RenkuUrlLoader[Try](config).failure.exception shouldBe an[ConfigLoadingException]
    }
  }
}
