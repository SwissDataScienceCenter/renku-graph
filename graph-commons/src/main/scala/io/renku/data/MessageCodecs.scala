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

import Message._
import cats.effect.Concurrent
import cats.syntax.all._
import io.circe.DecodingFailure.Reason.CustomReason
import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, EntityTypes, JsonLD, JsonLDEncoder}
import org.http4s.circe.{jsonEncoderOf, jsonOf}
import org.http4s.{EntityDecoder, EntityEncoder}

object MessageCodecs extends MessageCodecs

trait MessageCodecs {

  implicit lazy val severityDecoder: Decoder[Severity] = cur =>
    cur.as[String].flatMap {
      case Severity.Info.value  => Severity.Info.widen.asRight
      case Severity.Error.value => Severity.Error.widen.asRight
      case other =>
        DecodingFailure(CustomReason(s"unknown Message.Severity '$other'"), cur).asLeft
    }

  implicit lazy val severityEncoder: Encoder[Severity] = _.value.asJson

  implicit lazy val messageJsonDecoder: Decoder[Message] = Decoder.instance[Message] { cur =>
    def toMessage(severity: Severity): Json => Decoder.Result[Message] = { messageJson =>
      if (messageJson.isString)
        messageJson.as[String].map(Message.unsafeApply(_, severity))
      else if (severity == Message.Severity.Error)
        Message.Error.fromJsonUnsafe(messageJson).asRight
      else
        DecodingFailure(CustomReason(s"Malformed '$severity' Message with '$messageJson'"), cur).asLeft
    }

    cur.downField("severity").as[Message.Severity] >>= { severity =>
      cur.keys.toList.flatMap(_.filterNot(_ == "severity").toList) match {
        case "message" :: Nil =>
          cur.downField("message").as[Json] >>= toMessage(severity)
        case _ =>
          cur.downField("severity").delete.as[Json] >>= toMessage(severity)
      }
    }
  }

  implicit def messageJsonEncoder[T <: Message]: Encoder[T] = Encoder.instance[T] {
    case Message.StringMessage(v, s) => Json.obj("severity" -> s.asJson, "message" -> Json.fromString(v))
    case Message.JsonMessage(v, s)   => Json.obj("severity" -> s.asJson).deepMerge(v)
  }

  implicit def messageJsonEntityDecoder[F[_]: Concurrent]: EntityDecoder[F, Message] = jsonOf[F, Message]

  implicit def messageJsonEntityEncoder[F[_], T <: Message]: EntityEncoder[F, T] = jsonEncoderOf[F, T]

  implicit def messageJsonLDEncoder[T <: Message]: JsonLDEncoder[T] = JsonLDEncoder.instance[T] {
    case Message.StringMessage(value, severity) =>
      JsonLD.entity(
        EntityId.blank,
        toEntityTypes(severity),
        schema / "description" -> value.asJsonLD
      )
    case Message.JsonMessage(value, severity) =>
      JsonLD.entity(
        EntityId.blank,
        toEntityTypes(severity),
        schema / "description" -> value.noSpaces.asJsonLD
      )
  }

  private def toEntityTypes: Severity => EntityTypes = {
    case Severity.Error => EntityTypes of renku / "Error"
    case Severity.Info  => EntityTypes of renku / "Info"
  }
}
