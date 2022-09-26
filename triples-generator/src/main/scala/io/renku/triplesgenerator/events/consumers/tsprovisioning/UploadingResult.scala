/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsprovisioning

private trait UploadingResult[T <: UploadingResult[T]] extends Product with Serializable with UploadingResultAlg[T]

private trait UploadingError[T <: UploadingResult[T]] extends UploadingResult[T] {
  val cause: Throwable
}

private trait UploadingResultAlg[T <: UploadingResult[T]] {
  self: UploadingResult[T] =>
  def merge(other: UploadingResult[T]): T
}

private object UploadingResult {
  trait Uploaded[T <: UploadingResult[T]] extends UploadingResult[T] {
    override def merge(other: UploadingResult[T]): T = other match {
      case r: Uploaded[T]            => r.asInstanceOf[T]
      case r: RecoverableError[T]    => r.asInstanceOf[T]
      case _: NonRecoverableError[T] => this.asInstanceOf[T]
    }
  }

  trait RecoverableError[T <: UploadingResult[T]] extends UploadingError[T] {
    override def merge(other: UploadingResult[T]): T = other match {
      case _: Uploaded[T]            => this.asInstanceOf[T]
      case _: RecoverableError[T]    => this.asInstanceOf[T]
      case _: NonRecoverableError[T] => this.asInstanceOf[T]
    }
  }

  trait NonRecoverableError[T <: UploadingResult[T]] extends UploadingError[T] {
    override def merge(other: UploadingResult[T]): T = other match {
      case r: Uploaded[T]            => r.asInstanceOf[T]
      case r: RecoverableError[T]    => r.asInstanceOf[T]
      case _: NonRecoverableError[T] => this.asInstanceOf[T]
    }
  }
}
