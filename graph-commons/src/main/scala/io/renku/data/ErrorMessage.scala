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

package io.renku.data

import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.collection.NonEmpty

import java.io.{PrintWriter, StringWriter}

object ErrorMessage {

  type ErrorMessage = String Refined NonEmpty

  def apply(errorMessage: String): ErrorMessage = toErrorMessage {
    blankToNone(errorMessage)
      .map(toSingleLine)
      .getOrElse(throw new IllegalArgumentException("ErrorMessage cannot be instantiated with a blank String"))
  }

  def withExceptionMessage(exception: Throwable): ErrorMessage = toErrorMessage {
    blankToNone(exception.getMessage)
      .fold(ifEmpty = exception.getClass.getName)(toSingleLine)
  }

  def withStackTrace(exception: Throwable): ErrorMessage = toErrorMessage {
    blankToNone {
      val sw = new StringWriter
      exception.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
      .fold(ifEmpty = exception.getClass.getName)(toSingleLine)
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

  private val toErrorMessage: String => ErrorMessage = RefType
    .applyRef[ErrorMessage](_)
    .fold(
      error => throw new IllegalArgumentException(error),
      identity
    )
}
