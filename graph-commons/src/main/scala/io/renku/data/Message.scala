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

import cats.Show
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.circe.Json

import java.io.{PrintWriter, StringWriter}

sealed trait Message {
  type T
  val value:    T
  val severity: Message.Severity

  def show:                   String
  override lazy val toString: String = show
}

object Message {

  sealed trait Severity extends Product {
    lazy val widen:             Severity = this
    lazy val value:             String   = productPrefix
    override lazy val toString: String   = value
  }
  object Severity {
    case object Error extends Severity
    case object Info  extends Severity
    implicit def show[S <: Severity]: Show[S] = Show.fromToString
  }

  def unsafeApply(message: String, severity: Severity): Message =
    toStringMessageUnsafe(severity) {
      blankToNone(message)
        .map(toSingleLine)
        .getOrElse(throw new IllegalArgumentException("Message cannot be instantiated with a blank String"))
    }

  object Info {

    def apply(value: String Refined NonEmpty): Message =
      StringMessage(value.value, Severity.Info)

    def unsafeApply(message: String): Message =
      toStringMessageUnsafe(Severity.Info) {
        blankToNone(message)
          .map(toSingleLine)
          .getOrElse(throw new IllegalArgumentException("Message cannot be instantiated with a blank String"))
      }
  }

  object Error {

    def fromJsonUnsafe(errorMessage: Json): Message =
      if (errorMessage.isNull || errorMessage == Json.obj())
        throw new IllegalArgumentException("Message cannot be an empty Json")
      else
        JsonMessage(errorMessage, Severity.Error)

    def apply(value: String Refined NonEmpty): Message =
      StringMessage(value.value, Severity.Error)

    def unsafeApply(errorMessage: String): Message =
      toStringMessageUnsafe(Severity.Error) {
        blankToNone(errorMessage)
          .map(toSingleLine)
          .getOrElse(throw new IllegalArgumentException("Message cannot be instantiated with a blank String"))
      }

    def fromExceptionMessage(exception: Throwable): Message =
      toStringMessageUnsafe(Severity.Error) {
        blankToNone(exception.getMessage)
          .fold(ifEmpty = exception.getClass.getName)(toSingleLine)
      }

    def fromStackTrace(exception: Throwable): Message =
      toStringMessageUnsafe(Severity.Error) {
        blankToNone {
          val sw = new StringWriter
          exception.printStackTrace(new PrintWriter(sw))
          sw.toString
        }.fold(ifEmpty = exception.getClass.getName)(toSingleLine)
      }

    def fromMessageAndStackTraceUnsafe(message: String, exception: Throwable): Message =
      toStringMessageUnsafe(Severity.Error) {
        Option(exception)
          .flatMap { e =>
            blankToNone {
              val sw = new StringWriter
              e.printStackTrace(new PrintWriter(sw))
              s"$message; $sw"
            }
          }
          .fold(ifEmpty = message)(toSingleLine)
      }
  }

  private def blankToNone(message: String): Option[String] =
    Option(message).map(_.trim) >>= {
      case ""       => None
      case nonBlank => Some(nonBlank)
    }

  private lazy val toSingleLine: String => String =
    _.split('\n').map(_.trim).mkString("", "; ", "")

  private def toStringMessageUnsafe(severity: Severity): String => Message.StringMessage =
    _.trim match {
      case ""       => throw new IllegalArgumentException(show"$severity message cannot be blank")
      case nonBlank => StringMessage(nonBlank, severity)
    }

  private final case class StringMessage(value: String, severity: Severity) extends Message {
    override type T = String
    override lazy val show: String = value
  }
  private final case class JsonMessage(value: Json, severity: Severity) extends Message {
    override type T = Json
    override lazy val show: String = value.noSpaces
  }

  implicit def show[T <: Message]: Show[T] = Show.show[T] {
    case m: Message.StringMessage => m.show
    case m: Message.JsonMessage   => m.show
  }

  object Codecs {

    import cats.effect.Concurrent
    import io.circe.syntax._
    import io.circe.{Decoder, DecodingFailure, Encoder}
    import io.renku.graph.model.Schemas.{renku, schema}
    import io.renku.jsonld.syntax._
    import io.renku.jsonld.{EntityId, EntityTypes, JsonLD, JsonLDEncoder}
    import org.http4s.circe.{jsonEncoderOf, jsonOf}
    import org.http4s.{EntityDecoder, EntityEncoder}

    implicit lazy val severityDecoder: Decoder[Severity] = cur =>
      cur.as[String].flatMap {
        case Severity.Info.value  => Severity.Info.widen.asRight
        case Severity.Error.value => Severity.Error.widen.asRight
        case other =>
          DecodingFailure(DecodingFailure.Reason.CustomReason(s"unknown Message.Severity '$other'"), cur).asLeft
      }

    implicit lazy val severityEncoder: Encoder[Severity] = _.value.asJson

    implicit lazy val messageJsonDecoder: Decoder[Message] = Decoder.instance[Message] { cur =>
      for {
        severity    <- cur.downField("severity").as[Message.Severity]
        messageJson <- cur.downField("message").as[Json]
        message <- if (messageJson.isString) messageJson.as[String].map(Message.unsafeApply(_, severity))
                   else if (messageJson.isObject && severity == Message.Severity.Error)
                     Message.Error.fromJsonUnsafe(messageJson).asRight
                   else
                     DecodingFailure(
                       DecodingFailure.Reason.CustomReason(s"Malformed '$severity' Message with '$messageJson'"),
                       cur
                     ).asLeft
      } yield message
    }

    implicit def messageJsonEncoder[T <: Message]: Encoder[T] = Encoder.instance[T] {
      case Message.StringMessage(v, s) => Json.obj("severity" -> s.asJson, "message" -> Json.fromString(v))
      case Message.JsonMessage(v, s)   => Json.obj("severity" -> s.asJson, "message" -> v)
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
}
