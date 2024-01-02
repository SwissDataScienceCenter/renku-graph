/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.producers.triplesgenerated

import cats.Show
import cats.syntax.all._
import io.renku.graph.model.events.{CompoundEventId, ZippedEventPayload}
import io.renku.graph.model.projects

private final case class TriplesGeneratedEvent(id:          CompoundEventId,
                                               projectSlug: projects.Slug,
                                               payload:     ZippedEventPayload
) {
  override lazy val toString: String = s"$TriplesGeneratedEvent $id, projectSlug = $projectSlug"
}

private object TriplesGeneratedEvent {
  implicit lazy val show: Show[TriplesGeneratedEvent] =
    Show.show(event => show"${event.id}, projectSlug = ${event.projectSlug}")
}

private object TriplesGeneratedEventEncoder {

  import io.circe.Json
  import io.circe.literal.JsonStringContext

  lazy val encodeEvent: TriplesGeneratedEvent => Json = event => json"""{
    "categoryName": ${SubscriptionCategory.categoryName},
    "id":           ${event.id.id},
    "project": {
      "id":   ${event.id.projectId},
      "slug": ${event.projectSlug}
    }
  }"""

  lazy val encodePayload: TriplesGeneratedEvent => ZippedEventPayload = event => event.payload
}
