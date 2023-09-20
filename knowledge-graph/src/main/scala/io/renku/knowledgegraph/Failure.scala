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

package io.renku.knowledgegraph

import io.renku.core.client.Result.{Failure => CoreFailure}
import io.renku.data.Message
import org.http4s.{Response, Status}

private sealed trait Failure extends Exception {
  val status:  Status
  val message: Message
  def detailedMessage: String

  def toResponse[F[_]]: Response[F] =
    Response[F](status).withEntity(message)
}

private object Failure {

  final case class Simple(status: Status, message: Message) extends Exception(message.show) with Failure {
    override lazy val detailedMessage: String = getMessage
  }

  final case class WithCause(status: Status, message: Message, cause: Throwable)
      extends Exception(message.show, cause)
      with Failure {

    override lazy val detailedMessage: String = cause match {
      case f: CoreFailure => s"$getMessage; ${f.detailedMessage}"
      case _ => getMessage
    }
  }

  def apply(status: Status, message: Message): Failure =
    Failure.Simple(status, message)

  def apply(status: Status, message: Message, cause: Throwable): Failure =
    Failure.WithCause(status, message, cause)
}
