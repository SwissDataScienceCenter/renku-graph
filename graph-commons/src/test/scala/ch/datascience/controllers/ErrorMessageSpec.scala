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

package ch.datascience.controllers

import org.scalatest.Matchers._
import org.scalatest.WordSpec
import play.api.libs.json._

class ErrorMessageSpec extends WordSpec {

  "ErrorResponse" should {

    "be instantiatable from a non blank String" in {
      ErrorMessage("abc").value shouldBe "abc"
    }

    "throw an IllegalArgumentException for an empty String" in {
      intercept[IllegalArgumentException] {
        ErrorMessage("")
      }.getMessage shouldBe "Error message cannot be blank"
    }

    "throw an IllegalArgumentException for a blank String" in {
      intercept[IllegalArgumentException] {
        ErrorMessage(" ")
      }.getMessage shouldBe "Error message cannot be blank"
    }

    "be instantiatable from a JsError without path" in {
      val jsError = JsError("json error")
      ErrorMessage(jsError).value shouldBe "Json deserialization error(s): json error"
    }

    "be instantiatable from a JsError with path" in {
      val jsError = JsError(JsPath(List(KeyPathNode("key"))), "json error")
      ErrorMessage(jsError).value shouldBe "Json deserialization error(s): /key -> json error"
    }
  }

  "extension method toJson" should {

    import ErrorMessage._

    "deserialize ErrorMessage to JsValue" in {
      ErrorMessage("error message").toJson shouldBe Json.obj(
        "error" -> "error message"
      )
    }
  }
}
