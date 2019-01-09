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

import eu.timepit.refined.W
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.string.MatchesRegex
import play.api.libs.json._

object ErrorMessage {

  type ErrorMessage = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]

  def apply(errorMessage: String): ErrorMessage =
    RefType
      .applyRef[ErrorMessage](errorMessage)
      .fold(
        _ => throw new IllegalArgumentException("Error message cannot be blank"),
        identity
      )

  def apply(jsError: JsError): ErrorMessage = {
    val errorMessages = jsError.errors.map {
      case (JsPath(Nil), pathErrors) => pathErrors.map(_.message).mkString("; ")
      case (path, pathErrors)        => s"$path -> ${pathErrors.map(_.message).mkString("; ")}"
    }
    ErrorMessage(errorMessages.mkString("Json deserialization error(s): ", "; ", ""))
  }

  private implicit val errorResponseWrites: Writes[ErrorMessage] = Writes[ErrorMessage] { error =>
    Json.obj("error" -> error.value)
  }

  implicit class ErrorMessageOps(errorResponse: ErrorMessage) {
    lazy val toJson: JsValue = Json.toJson(errorResponse)
  }
}
