/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.core.client

import Generators.{resultDetailedFailures, resultSimpleFailures}
import io.renku.generators.Generators.Implicits._
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ResultSpec extends AnyWordSpec with should.Matchers with OptionValues {

  "Failure.Simple.detailedMessage" should {

    "return a String with the error" in {

      val failure = resultSimpleFailures.generateOne

      failure.detailedMessage shouldBe failure.error
    }
  }

  "Failure.Detailed.detailedMessage" should {

    "return a String with the userMessage and code if no devMessage is present" in {

      val failure = resultDetailedFailures.suchThat(_.maybeDevMessage.isEmpty).generateOne

      failure.detailedMessage shouldBe s"${failure.userMessage}: ${failure.code}"
    }

    "return a String with the userMessage and code if devMessage is present" in {

      val failure = resultDetailedFailures.suchThat(_.maybeDevMessage.nonEmpty).generateOne

      failure.detailedMessage shouldBe s"${failure.userMessage}: ${failure.code}; devMessage: ${failure.maybeDevMessage.value}"
    }
  }
}
