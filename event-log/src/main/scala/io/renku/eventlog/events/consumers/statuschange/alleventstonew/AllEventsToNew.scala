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

package io.renku.eventlog.events.consumers.statuschange
package alleventstonew

import cats.syntax.all._
import cats.Show
import io.circe.DecodingFailure
import io.renku.events.EventRequestContent
import io.renku.graph.model.events
import io.renku.graph.model.events.EventStatus

private[statuschange] trait AllEventsToNew extends StatusChangeEvent {
  def toRaw: RawStatusChangeEvent =
    RawStatusChangeEvent(None, None, None, None, None, EventStatus.New)
}

private[statuschange] case object AllEventsToNew extends AllEventsToNew {
  override val silent: Boolean = false

  val decoder: EventRequestContent => Either[DecodingFailure, AllEventsToNew] =
    _.event.hcursor.downField("newStatus").as[events.EventStatus] >>= {
      case events.EventStatus.New => Right(AllEventsToNew)
      case status                 => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
    }

  implicit lazy val show: Show[AllEventsToNew] = Show.show(_ => s"status = ${events.EventStatus.New}")

  def unapply(raw: RawStatusChangeEvent): Option[AllEventsToNew] =
    raw match {
      case RawStatusChangeEvent(_, None, _, _, _, EventStatus.New) => AllEventsToNew.some
      case _                                                       => None
    }
}
