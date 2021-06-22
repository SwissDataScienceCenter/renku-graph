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
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventProcessingTime}
import ch.datascience.graph.model.{SchemaVersion, projects}
import io.renku.eventlog.EventPayload

private sealed trait StatusChangeEvent extends Product with Serializable

private object StatusChangeEvent {

  final case class ToNew(eventId: CompoundEventId, projectPath: projects.Path) extends StatusChangeEvent
  object ToNew {
    implicit lazy val show: Show[ToNew] = Show.show { case ToNew(eventId, projectPath) =>
      s"$eventId, projectPath = $projectPath, status = $New"
    }
  }

  final case class ToTriplesGenerated(eventId:        CompoundEventId,
                                      projectPath:    projects.Path,
                                      processingTime: EventProcessingTime,
                                      payload:        EventPayload,
                                      schemaVersion:  SchemaVersion
  ) extends StatusChangeEvent
  object ToTriplesGenerated {
    implicit lazy val show: Show[ToTriplesGenerated] = Show.show {
      case ToTriplesGenerated(eventId, projectPath, _, _, _) =>
        s"$eventId, projectPath = $projectPath, status = $TriplesGenerated"
    }
  }

  final case class ToTriplesStore(eventId:        CompoundEventId,
                                  projectPath:    projects.Path,
                                  processingTime: EventProcessingTime
  ) extends StatusChangeEvent
  object ToTriplesStore {
    implicit lazy val show: Show[ToTriplesStore] = Show.show { case ToTriplesStore(eventId, projectPath, _) =>
      s"$eventId, projectPath = $projectPath, status = $TriplesStore"
    }
  }

  type AllEventsToNew = AllEventsToNew.type
  final case object AllEventsToNew extends StatusChangeEvent {
    implicit lazy val show: Show[AllEventsToNew] = Show.show { case AllEventsToNew =>
      s"All events status = $New"
    }
  }

  implicit def show[E <: StatusChangeEvent](implicit concreteShow: Show[E]): Show[E] = concreteShow
}
