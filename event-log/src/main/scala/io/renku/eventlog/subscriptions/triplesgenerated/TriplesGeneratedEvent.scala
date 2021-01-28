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

package io.renku.eventlog.subscriptions.triplesgenerated

import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.graph.model.{SchemaVersion, projects}
import cats.syntax.all._
import io.renku.eventlog.EventPayload
import io.renku.eventlog.subscriptions.EventEncoder

private final case class TriplesGeneratedEvent(id:            CompoundEventId,
                                               projectPath:   projects.Path,
                                               payload:       EventPayload,
                                               schemaVersion: SchemaVersion
) {
  override lazy val toString: String =
    s"$TriplesGeneratedEvent $id, projectPath = $projectPath"

}

private object TriplesGeneratedEventEncoder extends EventEncoder[TriplesGeneratedEvent] {

  import io.circe.Json
  import io.circe.literal.JsonStringContext
  // TODO add  project path
  override def encodeEvent(event: TriplesGeneratedEvent): Json = json"""{
    "categoryName": ${SubscriptionCategory.name.value},
    "id":           ${event.id.id.value},
    "project": {
      "id":         ${event.id.projectId.value},
      "path": ${event.projectPath.value}
    },
    "schemaVersion":${event.schemaVersion.value}
  }"""

  override def encodePayload(event: TriplesGeneratedEvent): Option[String] = event.payload.value.some
}
