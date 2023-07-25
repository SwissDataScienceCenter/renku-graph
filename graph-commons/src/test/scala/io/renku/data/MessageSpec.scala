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

import io.circe.Json
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import org.scalacheck.Gen
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.io.{PrintWriter, StringWriter}

class MessageSpec extends AnyWordSpec with should.Matchers with EitherValues {

  "Message.Info.apply" should {

    "be instantiatable from a NonEmpty String" in {
      val message = nonBlankStrings().generateOne
      Message.Info(message).value shouldBe message.value
    }
  }

  "Message.Info.unsafeApply" should {

    "be instantiatable from a non blank String" in {

      val line1                 = nonEmptyStrings().generateOne
      val (line2Message, line2) = tabbedLines.generateOne

      Message.Info.unsafeApply(s"$line1\n$line2").value shouldBe s"$line1; $line2Message"
    }

    "fail for a blank string" in {
      intercept[IllegalArgumentException](
        Message.Info.unsafeApply(blankStrings().generateOne)
      ).getMessage shouldBe "Message cannot be instantiated with a blank String"
    }
  }

  "Message.Error.fromJsonUnsafe" should {

    "return the JsonMessage" in {

      val message = jsons.generateOne

      Message.Error.fromJsonUnsafe(message).value shouldBe message
    }

    "fail for an empty on null JSON" in {

      intercept[IllegalArgumentException](
        Message.Error.fromJsonUnsafe(Json.Null)
      ).getMessage shouldBe "Message cannot be an empty Json"

      intercept[IllegalArgumentException](
        Message.Error.fromJsonUnsafe(Json.obj())
      ).getMessage shouldBe "Message cannot be an empty Json"
    }
  }

  "Message.Error.apply" should {

    "be instantiatable from a NonEmpty String" in {
      val message = nonBlankStrings().generateOne
      Message.Error(message).value shouldBe message.value
    }
  }

  "Message.Error.unsafeApply" should {

    "be instantiatable from a non blank String" in {

      val line1                 = nonEmptyStrings().generateOne
      val (line2Message, line2) = tabbedLines.generateOne

      Message.Error.unsafeApply(s"$line1\n$line2").value shouldBe s"$line1; $line2Message"
    }

    "fail for a blank string" in {
      intercept[IllegalArgumentException](
        Message.Error.unsafeApply(blankStrings().generateOne)
      ).getMessage shouldBe "Message cannot be instantiated with a blank String"
    }
  }

  "Message.Error.fromExceptionMessage" should {

    "contain a single-lined exception's message if non-null and non-blank" in {

      val exception = exceptions.generateOne

      val message = Message.Error.fromExceptionMessage(exception)

      message.value shouldBe exception.getMessage
    }

    "contain a single-lined exception's message if multi-lined" in {

      val line1                 = nonEmptyStrings().generateOne
      val (line2Message, line2) = tabbedLines.generateOne
      val exception             = new Exception(s"$line1\n$line2")

      Message.Error.fromExceptionMessage(exception).value shouldBe s"$line1; $line2Message"
    }

    "contain exception class name if exception with a null message" in {

      val exception = new Exception()
      assume(exception.getMessage == null)

      Message.Error.fromExceptionMessage(exception).value shouldBe s"${exception.getClass.getName}"
    }

    "contain exception class name if exception with a blank message" in {

      val exception = new Exception(blankStrings().generateOne)

      Message.Error.fromExceptionMessage(exception).value shouldBe s"${exception.getClass.getName}"
    }
  }

  "Message.Error.fromStackTrace" should {

    "contain a single-lined exception's stack trace" in {

      val exception = exceptions.generateOne

      val sw = new StringWriter
      exception.printStackTrace(new PrintWriter(sw))
      Message.Error.fromStackTrace(exception).value shouldBe sw.toString.split("\n").map(_.trim).mkString("; ")
    }
  }

  "Message.fromMessageAndStackTraceUnsafe" should {

    "contain a single-lined message and exception's stack trace if not null" in {

      val message   = sentences().generateOne.value
      val exception = exceptions.generateOne

      val sw = new StringWriter
      exception.printStackTrace(new PrintWriter(sw))

      Message.Error
        .fromMessageAndStackTraceUnsafe(message, exception)
        .value shouldBe s"$message; ${sw.toString.split("\n").map(_.trim).mkString("; ")}"
    }

    "contain the message only for null exception" in {

      val message = sentences().generateOne.value

      Message.Error.fromMessageAndStackTraceUnsafe(message, exception = null).value shouldBe s"$message"
    }

    "fail for a null exception and blank message" in {
      intercept[IllegalArgumentException](
        Message.Error.fromMessageAndStackTraceUnsafe(blankStrings().generateOne, exception = null)
      ).getMessage shouldBe "Error message cannot be blank"
    }
  }

  "Message.show & toString" should {

    "return value in case of a StringMessage" in {

      val message = nonBlankStrings().generateOne

      Message.Info(message).show     shouldBe message.value
      Message.Info(message).toString shouldBe message.value

      Message.Error(message).show     shouldBe message.value
      Message.Error(message).toString shouldBe message.value
    }

    "return value in case of a JsonMessage" in {

      val message = jsons.generateOne

      Message.Error.fromJsonUnsafe(message).show     shouldBe message.noSpaces
      Message.Error.fromJsonUnsafe(message).toString shouldBe message.noSpaces
    }
  }

  private lazy val tabbedLines: Gen[(String, String)] = for {
    lineTabbing <- nonEmptyList(Gen.const(' ')).map(_.toList.mkString(""))
    lineMessage <- nonEmptyStrings()
  } yield lineMessage -> s"$lineTabbing$lineMessage"
}
