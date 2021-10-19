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

import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.collection.NonEmpty
import io.circe._
import io.renku.data.{ErrorMessage => DataErrorMessage}
import org.http4s.EntityEncoder
import org.http4s.circe.jsonEncoderOf

object ErrorMessage {

  type ErrorMessage = DataErrorMessage.ErrorMessage

  def apply(errorMessage: String): ErrorMessage = DataErrorMessage(errorMessage)

  def apply(exception: Throwable): ErrorMessage = DataErrorMessage.withExceptionMessage(exception)

  implicit val errorMessageEncoder: Encoder[ErrorMessage] = Encoder.instance[ErrorMessage] { message =>
    Json.obj("message" -> Json.fromString(message.value))
  }

  implicit def errorMessageEntityEncoder[F[_]]: EntityEncoder[F, ErrorMessage] = jsonEncoderOf[F, ErrorMessage]
}

object InfoMessage {

  type InfoMessage = String Refined NonEmpty

  def apply(message: String): InfoMessage =
    RefType
      .applyRef[InfoMessage](message)
      .fold(
        _ => throw new IllegalArgumentException("Error message cannot be blank"),
        identity
      )

  implicit val infoMessageEncoder: Encoder[InfoMessage] = Encoder.instance[InfoMessage] { message =>
    Json.obj("message" -> Json.fromString(message.value))
  }

  implicit def infoMessageEntityEncoder[F[_]]: EntityEncoder[F, InfoMessage] = jsonEncoderOf[F, InfoMessage]
}
