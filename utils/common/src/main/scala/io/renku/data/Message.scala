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

import cats.Show
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json}

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
    lazy val value:             String   = productPrefix.toLowerCase
    override lazy val toString: String   = value
  }
  object Severity {
    case object Error extends Severity
    case object Info  extends Severity
    implicit def show[S <: Severity]: Show[S] = Show.fromToString

    def fromString(str: String): Either[String, Severity] =
      List(Error, Info)
        .find(_.value.equalsIgnoreCase(str))
        .toRight(s"unknown Message.Severity '$str'")

    implicit lazy val severityDecoder: Decoder[Severity] = Decoder.decodeString.emap(fromString)
    implicit lazy val severityEncoder: Encoder[Severity] = Encoder.encodeString.contramap(_.value)
  }

  def unsafeApply(message: String, severity: Severity): Message =
    toStringMessageUnsafe(severity) {
      blankToNone(message)
        .map(toSingleLine)
        .getOrElse(throw new IllegalArgumentException("Message cannot be instantiated with a blank String"))
    }

  def fromJsonUnsafe(message: Json, severity: Severity): Message =
    JsonMessage(message, severity)

  object Info {

    def apply(value: String Refined NonEmpty): Message =
      StringMessage(value.value, Severity.Info)

    def unsafeApply(message: String): Message =
      toStringMessageUnsafe(Severity.Info) {
        blankToNone(message)
          .map(toSingleLine)
          .getOrElse(throw new IllegalArgumentException("Message cannot be instantiated with a blank String"))
      }

    def fromJsonUnsafe(message: Json): Message =
      JsonMessage(failIfEmpty(message), Severity.Info)
  }

  object Error {

    def fromJsonUnsafe(message: Json): Message =
      JsonMessage(failIfEmpty(message), Severity.Error)

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

  private def failIfEmpty(message: Json): Json = {
    val sanitized = message.deepDropNullValues
    if (sanitized.isNull || sanitized == Json.obj() || sanitized == Json.arr())
      throw new IllegalArgumentException("Message cannot be an empty Json")
    message
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
      case ""       => throw new IllegalArgumentException(show"${severity.show.capitalize} message cannot be blank")
      case nonBlank => StringMessage(nonBlank, severity)
    }

  private[data] final case class StringMessage(value: String, severity: Severity) extends Message {
    override type T = String
    override lazy val show: String = value
  }
  private[data] final case class JsonMessage(value: Json, severity: Severity) extends Message {
    override type T = Json
    override lazy val show: String = value.noSpaces
  }

  implicit def show[T <: Message]: Show[T] = Show.show[T] {
    case m: Message.StringMessage => m.show
    case m: Message.JsonMessage   => m.show
  }

  implicit lazy val messageJsonDecoder: Decoder[Message] = Decoder.instance[Message] { cur =>
    def toMessage(severity: Severity): Json => Decoder.Result[Message] = { messageJson =>
      if (messageJson.isString)
        messageJson.as[String].map(Message.unsafeApply(_, severity))
      else
        Message.fromJsonUnsafe(messageJson, severity).asRight
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
}
