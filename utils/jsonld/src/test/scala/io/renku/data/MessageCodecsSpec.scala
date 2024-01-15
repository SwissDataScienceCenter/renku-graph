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

package io.renku.data

import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class MessageCodecsSpec extends AnyWordSpec with should.Matchers with EitherValues {

  "Message.Error codecs" should {
    "provide Json codecs" in {

      val message = Message.Error.fromExceptionMessage(new Exception(blankStrings().generateOne))

      message.asJson.as[Message].value shouldBe message
    }

    "provide Json encoder producing a Json object with the message in the 'message' property" in {

      val exception = new Exception(blankStrings().generateOne)

      Message.Error.fromExceptionMessage(exception).asJson shouldBe json"""{
        "severity": ${Message.Severity.Error.value},
        "message":  ${exception.getClass.getName}
      }"""
    }

    "provide Json encoder that merges the Json body into the output Json" in {

      val jsonBody = jsons.generateOne

      Message.Error.fromJsonUnsafe(jsonBody).asJson shouldBe json"""{
        "severity": ${Message.Severity.Error.widen}
      }""".deepMerge(jsonBody)
    }
  }

  "Message.Info encoder" should {
    "provide Json codecs" in {

      val message = Message.Info(nonBlankStrings().generateOne)

      message.asJson.as[Message].value shouldBe message
    }

    "provide Json encoder producing a Json object with the message in the 'message' property" in {

      val value = nonBlankStrings().generateOne

      Message.Info(value).asJson shouldBe json"""{
        "severity": ${Message.Severity.Info.value},
        "message":  ${value.value}
      }"""
    }

    "provide Json encoder that merges the Json body into the output Json" in {

      val jsonBody = jsons.generateOne

      Message.Info.fromJsonUnsafe(jsonBody).asJson shouldBe
        json"""{
          "severity": ${Message.Severity.Info.widen}
        }""".deepMerge(jsonBody)
    }
  }
}
