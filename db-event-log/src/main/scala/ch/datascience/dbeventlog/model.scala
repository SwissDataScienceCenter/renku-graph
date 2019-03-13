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
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}

final class EventBody private (val value: String) extends AnyVal with TinyType[String]
object EventBody extends TinyTypeFactory[String, EventBody](new EventBody(_)) with NonBlank

final class CreatedDate private (val value: Instant) extends AnyVal with TinyType[Instant]
object CreatedDate extends TinyTypeFactory[Instant, CreatedDate](new CreatedDate(_)) with InstantNotInTheFuture

final class ExecutionDate private (val value: Instant) extends AnyVal with TinyType[Instant]
object ExecutionDate extends TinyTypeFactory[Instant, ExecutionDate](new ExecutionDate(_))

final class Message private (val value: String) extends AnyVal with TinyType[String]
object Message extends TinyTypeFactory[String, Message](new Message(_)) with NonBlank

sealed trait EventStatus extends TinyType[String] with Product with Serializable
object EventStatus extends TinyTypeFactory[String, EventStatus](EventStatusInstantiator) {
  final case object New extends EventStatus {
    override val value: String = "NEW"
  }
}
private object EventStatusInstantiator extends (String => EventStatus) {
  override def apply(value: String): EventStatus = value match {
    case EventStatus.New.value => EventStatus.New
    case other                 => throw new IllegalArgumentException(s"'$other' unknown EventStatus")
  }
}
