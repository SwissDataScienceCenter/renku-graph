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

package io.renku.data

import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.io.{PrintWriter, StringWriter}

class ErrorMessageSpec extends AnyWordSpec with should.Matchers {

  "ErrorMessage" should {

    "be instantiatable from a non blank String" in {
      val line1                 = nonEmptyStrings().generateOne
      val (line2Message, line2) = tabbedLines.generateOne

      ErrorMessage(s"$line1\n$line2").value.value shouldBe s"$line1; $line2Message"
    }
  }

  "ErrorMessage.withExceptionMessage" should {

    "be instantiable from an Exception with a non-null, non-blank, single line message" in {

      val exception = exceptions.generateOne

      val message = ErrorMessage.withExceptionMessage(exception)

      message.value.value shouldBe exception.getMessage
    }

    "be instantiable from an Exception with a multi-line message having some tabbing" in {

      val line1                 = nonEmptyStrings().generateOne
      val (line2Message, line2) = tabbedLines.generateOne
      val exception             = new Exception(s"$line1\n$line2")

      val message = ErrorMessage.withExceptionMessage(exception)

      message.value.value shouldBe s"$line1; $line2Message"
    }

    "be instantiable from an Exception with a null message" in {

      val exception = new Exception()
      assume(exception.getMessage == null)

      val message = ErrorMessage.withExceptionMessage(exception)

      message.value.value shouldBe s"${exception.getClass.getName}"
    }

    "be instantiable from an Exception with a blank message" in {

      val exception = new Exception(blankStrings().generateOne)

      val message = ErrorMessage.withExceptionMessage(exception)

      message.value.value shouldBe s"${exception.getClass.getName}"
    }
  }

  "ErrorMessage.withStackTrace" should {

    "be instantiable from an Exception with a non-null, non-blank, single line message" in {

      val exception = exceptions.generateOne

      val message = ErrorMessage.withStackTrace(exception)

      val sw = new StringWriter
      exception.printStackTrace(new PrintWriter(sw))
      message.value.value shouldBe sw.toString.split("\n").map(_.trim).mkString("; ")
    }
  }

  "ErrorMessage.withMessageAndStackTrace" should {

    "be instantiable from the given message and Exception with a non-null, non-blank, single line message" in {

      val message   = sentences().generateOne.value
      val exception = exceptions.generateOne

      val actual = ErrorMessage.withMessageAndStackTrace(message, exception)

      val sw = new StringWriter
      exception.printStackTrace(new PrintWriter(sw))
      actual.value.value shouldBe s"$message; ${sw.toString.split("\n").map(_.trim).mkString("; ")}"
    }

    "return the message only for null exception" in {

      val message = sentences().generateOne.value

      val actual = ErrorMessage.withMessageAndStackTrace(message, exception = null)

      actual.value.value shouldBe s"$message"
    }
  }

  "ErrorMessage(Json)" should {

    "return the JsonMessage" in {

      val message = jsons.generateOne

      ErrorMessage(message).value shouldBe message
    }
  }

  "ErrorMessage.show & toString" should {

    "return value in case of a StringMessage" in {

      val message = nonBlankStrings().generateOne

      ErrorMessage(message.value).show shouldBe message.value
      ErrorMessage(message.value).toString shouldBe message.value
    }

    "return value in case of a JsonMessage" in {

      val message = jsons.generateOne

      ErrorMessage(message).show shouldBe message.noSpaces
      ErrorMessage(message).toString shouldBe message.noSpaces
    }
  }

  private lazy val tabbedLines: Gen[(String, String)] = for {
    lineTabbing <- nonEmptyList(Gen.const(' ')).map(_.toList.mkString(""))
    lineMessage <- nonEmptyStrings()
  } yield lineMessage -> s"$lineTabbing$lineMessage"
}
