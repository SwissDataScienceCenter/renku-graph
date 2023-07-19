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

package io.renku.http

import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.collection.NonEmpty
import io.circe._
import io.renku.data.{ErrorMessage => DataErrorMessage}
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.jsonld._
import io.renku.jsonld.syntax._
import org.http4s.EntityEncoder
import org.http4s.circe.jsonEncoderOf

object ErrorMessage {

  type ErrorMessage = DataErrorMessage

  def apply(errorMessage: Json): DataErrorMessage.JsonMessage = DataErrorMessage(errorMessage)

  def apply(errorMessage: String): DataErrorMessage.StringMessage = DataErrorMessage(errorMessage)

  def apply(exception: Throwable): DataErrorMessage.StringMessage = DataErrorMessage.withExceptionMessage(exception)

  implicit def errorMessageJsonEncoder[T <: ErrorMessage]: Encoder[T] = Encoder.instance[T] {
    case m: DataErrorMessage.StringMessage =>
      Json.obj("message" -> Json.fromString(m.value.value))
    case DataErrorMessage.JsonMessage(value: Json) =>
      Json.obj("message" -> value)
  }

  implicit def errorMessageJsonEntityEncoder[F[_], T <: ErrorMessage]: EntityEncoder[F, T] = jsonEncoderOf[F, T]

  implicit def errorMessageJsonLDEncoder[T <: ErrorMessage]: JsonLDEncoder[T] = JsonLDEncoder.instance[T] {
    case DataErrorMessage.StringMessage(value) =>
      JsonLD.entity(
        EntityId.blank,
        EntityTypes of renku / "Error",
        schema / "description" -> value.value.asJsonLD
      )
    case DataErrorMessage.JsonMessage(value: Json) =>
      JsonLD.entity(
        EntityId.blank,
        EntityTypes of renku / "Error",
        schema / "description" -> value.noSpaces.asJsonLD
      )
  }
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

  implicit val messageJsonEncoder: Encoder[InfoMessage] = Encoder.instance[InfoMessage] { message =>
    Json.obj("message" -> Json.fromString(message.value))
  }

  implicit def messageJsonEntityEncoder[F[_]]: EntityEncoder[F, InfoMessage] = jsonEncoderOf[F, InfoMessage]

  implicit val messageJsonLDEncoder: JsonLDEncoder[InfoMessage] = JsonLDEncoder.instance[InfoMessage] { message =>
    JsonLD.entity(
      EntityId.blank,
      EntityTypes.of(renku / "Info"),
      schema / "description" -> message.value.asJsonLD
    )
  }
}
