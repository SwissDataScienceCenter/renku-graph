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

package io.renku.eventlog.events.categories.statuschange

import cats.Show
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.graph.model.projects

private sealed trait StatusChangeEvent extends Product with Serializable

private object StatusChangeEvent {
  final case class AncestorsToTriplesGenerated(eventId: CompoundEventId, projectPath: projects.Path)
      extends StatusChangeEvent

  object AncestorsToTriplesGenerated {
    implicit lazy val show: Show[AncestorsToTriplesGenerated] = Show.show {
      case AncestorsToTriplesGenerated(eventId, projectPath) =>
        s"$eventId, projectPath = $projectPath, status = TRIPLES_GENERATED"
    }
  }

  final case class AncestorsToTriplesStore(eventId: CompoundEventId, projectPath: projects.Path)
      extends StatusChangeEvent

  object AncestorsToTriplesStore {
    implicit lazy val show: Show[AncestorsToTriplesStore] = Show.show {
      case AncestorsToTriplesStore(eventId, projectPath) =>
        s"$eventId, projectPath = $projectPath, status = TRIPLE_STORE"
    }
  }

  type AllEventsToNew = AllEventsToNew.type
  final case object AllEventsToNew extends StatusChangeEvent {
    implicit lazy val show: Show[AllEventsToNew] = Show.show { case AllEventsToNew =>
      s"All events status = NEW"
    }
  }

  implicit def show[E <: StatusChangeEvent](implicit concreteShow: Show[E]): Show[E] = concreteShow
}
