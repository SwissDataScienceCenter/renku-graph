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

package io.renku.eventlog.api.events

import cats.Show
import cats.syntax.all._
import io.circe.Encoder
import io.circe.syntax._
import io.renku.events.producers.EventSender
import io.renku.events.producers.EventSender.EventContext
import io.renku.events.{CategoryName, EventRequestContent}

class Dispatcher[F[_], E](categoryName: CategoryName)(implicit enc: Encoder[E], show: Show[E]) {

  def dispatch(event: E, eventSender: EventSender[F]): F[Unit] =
    eventSender.sendEvent(
      EventRequestContent.NoPayload(event.asJson),
      EventContext(categoryName, show"$categoryName: sending event $event failed")
    )
}

object Dispatcher {
  def instance[F[_], E](categoryName: CategoryName)(implicit enc: Encoder[E], show: Show[E]) =
    new Dispatcher[F, E](categoryName)
}
