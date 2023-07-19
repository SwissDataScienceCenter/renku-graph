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
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.collection.NonEmpty
import io.circe.Json

import java.io.{PrintWriter, StringWriter}

sealed trait ErrorMessage {
  type T
  val value: T
  def show: String
}

object ErrorMessage {

  final case class StringMessage(value: String Refined NonEmpty) extends ErrorMessage {
    override type T = String Refined NonEmpty
    override lazy val show: String = value.value
    override def equals(obj: Any): Boolean = obj match {
      case StringMessage(v) => v.value == value.value
    }
  }
  final case class JsonMessage(value: Json) extends ErrorMessage {
    override type T = Json
    override lazy val show: String = value.noSpaces
  }

  def apply(errorMessage: Json): ErrorMessage.JsonMessage =
    if (errorMessage.isNull || errorMessage == Json.obj())
      throw new IllegalArgumentException("ErrorMessage cannot be an empty Json")
    else
      JsonMessage(errorMessage)

  def apply(errorMessage: String): ErrorMessage.StringMessage = toErrorMessage {
    blankToNone(errorMessage)
      .map(toSingleLine)
      .getOrElse(throw new IllegalArgumentException("ErrorMessage cannot be instantiated with a blank String"))
  }

  def withExceptionMessage(exception: Throwable): ErrorMessage.StringMessage = toErrorMessage {
    blankToNone(exception.getMessage)
      .fold(ifEmpty = exception.getClass.getName)(toSingleLine)
  }

  def withStackTrace(exception: Throwable): ErrorMessage.StringMessage = toErrorMessage {
    blankToNone {
      val sw = new StringWriter
      exception.printStackTrace(new PrintWriter(sw))
      sw.toString
    }.fold(ifEmpty = exception.getClass.getName)(toSingleLine)
  }

  def withMessageAndStackTrace(message: String, exception: Throwable): ErrorMessage.StringMessage = toErrorMessage {
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

  private def blankToNone(message: String): Option[String] =
    Option(message)
      .map(_.trim)
      .flatMap {
        case ""       => None
        case nonBlank => Some(nonBlank)
      }

  private lazy val toSingleLine: String => String =
    _.split('\n').map(_.trim).mkString("", "; ", "")

  private val toErrorMessage: String => ErrorMessage.StringMessage = RefType
    .applyRef[String Refined NonEmpty](_)
    .fold(
      error => throw new IllegalArgumentException(error),
      StringMessage
    )

  implicit def show[T <: ErrorMessage]: Show[T] = Show.show[T] {
    case m: ErrorMessage.StringMessage => m.show
    case m: ErrorMessage.JsonMessage   => m.show
  }
}
