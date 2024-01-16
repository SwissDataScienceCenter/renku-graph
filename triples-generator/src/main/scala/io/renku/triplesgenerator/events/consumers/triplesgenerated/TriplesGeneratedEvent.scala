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

package io.renku.triplesgenerator.events.consumers.triplesgenerated

import cats.Show
import io.renku.events.consumers.Project
import io.renku.graph.model.events.{CompoundEventId, EventId}
import io.renku.jsonld.JsonLD
import io.renku.triplesgenerator.events.consumers.CategoryEvent

private final case class TriplesGeneratedEvent(eventId: EventId, project: Project, payload: JsonLD)
    extends Product
    with CategoryEvent {
  override val compoundEventId: CompoundEventId = CompoundEventId(eventId, project.id)
}

private object TriplesGeneratedEvent {
  implicit val show: Show[TriplesGeneratedEvent] = Show.show { event =>
    s"${event.compoundEventId}, projectSlug = ${event.project.slug}"
  }
}
