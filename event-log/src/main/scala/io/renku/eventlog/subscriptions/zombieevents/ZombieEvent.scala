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

import cats.Show
import cats.implicits.showInterpolator
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import io.renku.eventlog.subscriptions.EventEncoder

private final class ZombieEventProcess private (val value: String) extends AnyVal with StringTinyType
private object ZombieEventProcess extends TinyTypeFactory[ZombieEventProcess](new ZombieEventProcess(_))

private case class ZombieEvent(generatedBy: ZombieEventProcess,
                               eventId:     CompoundEventId,
                               projectPath: projects.Path,
                               status:      EventStatus
) {
  override lazy val toString: String =
    s"$ZombieEvent $generatedBy $eventId, projectPath = $projectPath, status = $status"
}

private object ZombieEvent {
  implicit lazy val show: Show[ZombieEvent] = Show.show(event =>
    show"ZombieEvent ${event.generatedBy} ${event.eventId}, projectPath = ${event.projectPath}, status = ${event.status}"
  )
}

private object ZombieEventEncoder extends EventEncoder[ZombieEvent] {

  import io.circe.Json
  import io.circe.literal.JsonStringContext

  override def encodeEvent(event: ZombieEvent): Json = json"""{
    "categoryName": ${categoryName.value},
    "id":           ${event.eventId.id.value},
    "project": {
      "id":         ${event.eventId.projectId.value},
      "path":       ${event.projectPath.value}
    },
    "status":       ${event.status.value}
  }"""

  override def encodePayload(categoryEvent: ZombieEvent): Option[String] = None
}
