/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.dbeventlog

import java.time.Instant

import ch.datascience.tinytypes.constraints.{InstantNotInTheFuture, NonBlank}
import ch.datascience.tinytypes.{InstantTinyType, StringTinyType, TinyTypeFactory}

final class EventBody private (val value: String) extends AnyVal with StringTinyType
object EventBody extends TinyTypeFactory[EventBody](new EventBody(_)) with NonBlank

final class CreatedDate private (val value: Instant) extends AnyVal with InstantTinyType
object CreatedDate extends TinyTypeFactory[CreatedDate](new CreatedDate(_)) with InstantNotInTheFuture

final class ExecutionDate private (val value: Instant) extends AnyVal with InstantTinyType
object ExecutionDate extends TinyTypeFactory[ExecutionDate](new ExecutionDate(_))

final class EventMessage private (val value: String) extends AnyVal with StringTinyType
object EventMessage extends TinyTypeFactory[EventMessage](new EventMessage(_)) with NonBlank {

  import java.io.{PrintWriter, StringWriter}

  def apply(exception: Throwable): Option[EventMessage] = {
    val exceptionAsString = new StringWriter
    exception.printStackTrace(new PrintWriter(exceptionAsString))
    exceptionAsString.flush()

    from(exceptionAsString.toString).fold(
      _ => None,
      Option.apply
    )
  }
}

sealed trait EventStatus extends StringTinyType with Product with Serializable
object EventStatus extends TinyTypeFactory[EventStatus](EventStatusInstantiator) {
  final case object New extends EventStatus {
    override val value: String = "NEW"
  }
  final case object Processing extends EventStatus {
    override val value: String = "PROCESSING"
  }
  final case object TriplesStore extends EventStatus {
    override val value: String = "TRIPLES_STORE"
  }

  sealed trait FailureStatus extends EventStatus

  final case object TriplesStoreFailure extends FailureStatus {
    override val value: String = "TRIPLES_STORE_FAILURE"
  }
  type TriplesStoreFailure = TriplesStoreFailure.type

  final case object NonRecoverableFailure extends FailureStatus {
    override val value: String = "NON_RECOVERABLE_FAILURE"
  }
  type NonRecoverableFailure = NonRecoverableFailure.type
}
private object EventStatusInstantiator extends (String => EventStatus) {
  import EventStatus._

  private val allStatuses = Set(New, Processing, TriplesStoreFailure, TriplesStore, NonRecoverableFailure)

  override def apply(value: String): EventStatus = allStatuses.find(_.value == value).getOrElse {
    throw new IllegalArgumentException(s"'$value' unknown EventStatus")
  }
}
