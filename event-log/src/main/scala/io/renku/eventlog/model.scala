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

package io.renku.eventlog

import ch.datascience.data.ErrorMessage
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints.{BoundedInstant, InstantNotInTheFuture, NonBlank}
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import io.circe.Decoder

import java.time.Instant

trait CompoundId {
  def compoundEventId: CompoundEventId
}

final class EventDate private (val value: Instant) extends AnyVal with InstantTinyType
object EventDate extends TinyTypeFactory[EventDate](new EventDate(_)) with BoundedInstant {
  import java.time.temporal.ChronoUnit.HOURS

  protected[this] override def maybeMax: Option[Instant] = Some(instantNow.plus(24, HOURS))

  implicit val decoder: Decoder[EventDate] = instantDecoder(EventDate)
}

final class CreatedDate private (val value: Instant) extends AnyVal with InstantTinyType
object CreatedDate extends TinyTypeFactory[CreatedDate](new CreatedDate(_)) with InstantNotInTheFuture

final class ExecutionDate private (val value: Instant) extends AnyVal with InstantTinyType
object ExecutionDate extends TinyTypeFactory[ExecutionDate](new ExecutionDate(_)) {
  implicit val decoder: Decoder[ExecutionDate] = instantDecoder(ExecutionDate)
}

final class EventMessage private (val value: String) extends AnyVal with StringTinyType

object EventMessage extends TinyTypeFactory[EventMessage](new EventMessage(_)) with NonBlank {

  implicit val decoder: Decoder[EventMessage] = stringDecoder(EventMessage)

  def apply(exception: Throwable): EventMessage = EventMessage(ErrorMessage.withStackTrace(exception).value)
}

final class EventPayload private (val value: Array[Byte]) extends AnyVal with ByteArrayTinyType

object EventPayload extends TinyTypeFactory[EventPayload](new EventPayload(_))
