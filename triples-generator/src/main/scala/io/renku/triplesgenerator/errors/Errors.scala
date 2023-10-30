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

package io.renku.triplesgenerator.errors

sealed abstract class ProcessingRecoverableError(val message: String, val cause: Throwable)
    extends Exception(message, cause)

object ProcessingRecoverableError {

  final case class LogWorthyRecoverableError(override val message: String, override val cause: Throwable)
      extends ProcessingRecoverableError(message, cause)
  object LogWorthyRecoverableError {
    def apply(message: String): LogWorthyRecoverableError = LogWorthyRecoverableError(message, null)
  }

  final case class SilentRecoverableError(override val message: String, override val cause: Throwable)
      extends ProcessingRecoverableError(message, cause)
  object SilentRecoverableError {
    def apply(message: String): SilentRecoverableError = SilentRecoverableError(message, null)
  }
}

sealed abstract class ProcessingNonRecoverableError(message: String, cause: Throwable)
    extends Exception(message, cause) {
  lazy val widen: ProcessingNonRecoverableError = this
}
object ProcessingNonRecoverableError {

  final case class MalformedRepository(message: String, cause: Throwable)
      extends ProcessingNonRecoverableError(message, cause)
  object MalformedRepository {
    def apply(message: String): MalformedRepository = MalformedRepository(message, null)
  }
}
