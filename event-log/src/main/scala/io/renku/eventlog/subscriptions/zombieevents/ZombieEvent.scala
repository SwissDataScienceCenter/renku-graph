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

package io.renku.eventlog.subscriptions.zombieevents

import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import io.circe.Encoder

private case class ZombieEvent(eventId: CompoundEventId, status: EventStatus) {
  override lazy val toString: String = s"$ZombieEvent $eventId, status = $status"
}

private object ZombieEventEncoder extends Encoder[ZombieEvent] {

  import io.circe.Json
  import io.circe.literal.JsonStringContext

  override def apply(event: ZombieEvent): Json = json"""{
    "categoryName": ${categoryName.value},
    "id":           ${event.eventId.id.value},
    "project": {
      "id":         ${event.eventId.projectId.value}
    },
    "status":       ${event.status.value}
  }"""
}
