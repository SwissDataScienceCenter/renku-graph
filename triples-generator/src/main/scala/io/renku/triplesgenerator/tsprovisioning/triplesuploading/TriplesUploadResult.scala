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

package io.renku.triplesgenerator.tsprovisioning.triplesuploading

import io.renku.triplesgenerator.errors.ProcessingRecoverableError

sealed trait TriplesUploadResult extends Product with Serializable {
  val message: String
}

object TriplesUploadResult {

  type DeliverySuccess = DeliverySuccess.type
  final case object DeliverySuccess extends TriplesUploadResult {
    val message: String = "Delivery success"
  }

  sealed trait TriplesUploadFailure extends Exception with TriplesUploadResult
  final case class RecoverableFailure(error: ProcessingRecoverableError)
      extends Exception(error)
      with TriplesUploadFailure {
    override val message: String = error.getMessage
  }
  sealed trait NonRecoverableFailure extends Exception with TriplesUploadFailure
  object NonRecoverableFailure {
    private case class NonRecoverableFailureWithCause(message: String, cause: Throwable)
        extends Exception(message, cause)
        with NonRecoverableFailure
    private case class NonRecoverableFailureWithoutCause(message: String)
        extends Exception(message)
        with NonRecoverableFailure

    def apply(message: String, cause: Throwable): NonRecoverableFailure = NonRecoverableFailureWithCause(message, cause)
    def apply(message: String): NonRecoverableFailure = NonRecoverableFailureWithoutCause(message)

    def unapply(nonRecoverableFailure: NonRecoverableFailure): Option[(String, Option[Throwable])] =
      nonRecoverableFailure match {
        case NonRecoverableFailureWithCause(message, cause) => Some(message, Some(cause))
        case NonRecoverableFailureWithoutCause(message)     => Some(message, None)
      }
  }
}
