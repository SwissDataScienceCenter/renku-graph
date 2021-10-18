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

package io.renku.http

import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ErrorMessageSpec extends AnyWordSpec with should.Matchers {

  "ErrorMessage" should {

    "be instantiatable from a non blank String" in {
      val line1                 = nonEmptyStrings().generateOne
      val (line2Message, line2) = tabbedLines.generateOne

      ErrorMessage(s"$line1\n$line2").value shouldBe s"$line1; $line2Message"
    }

    "be instantiable from an Exception with a non-null, non-blank, single line message" in {
      val exception = exceptions.generateOne

      val message = ErrorMessage(exception)

      message.value shouldBe exception.getMessage
    }

    "be instantiable from an Exception with a multi-line message having some tabbing" in {
      val line1                 = nonEmptyStrings().generateOne
      val (line2Message, line2) = tabbedLines.generateOne
      val exception             = new Exception(s"$line1\n$line2")

      val message = ErrorMessage(exception)

      message.value shouldBe s"$line1; $line2Message"
    }

    "be instantiable from an Exception with a null message" in {
      val exception = new Exception()
      assume(exception.getMessage == null)

      val message = ErrorMessage(exception)

      message.value shouldBe s"${exception.getClass.getName}"
    }

    "be instantiable from an Exception with a blank message" in {
      val exception = new Exception(blankStrings().generateOne)

      val message = ErrorMessage(exception)

      message.value shouldBe s"${exception.getClass.getName}"
    }
  }

  private lazy val tabbedLines: Gen[(String, String)] = for {
    lineTabbing <- nonEmptyList(Gen.const(' ')).map(_.toList.mkString(""))
    lineMessage <- nonEmptyStrings()
  } yield lineMessage -> s"$lineTabbing$lineMessage"
}
