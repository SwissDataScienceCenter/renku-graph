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
import ch.datascience.graph.model.events.{CompoundEventId, EventProcessingTime, ZippedEventPayload}
import ch.datascience.graph.model.projects
import io.renku.eventlog.EventMessage

private sealed trait StatusChangeEvent extends Product with Serializable

private object StatusChangeEvent {

  final case class RollbackToNew(eventId: CompoundEventId, projectPath: projects.Path) extends StatusChangeEvent
  object RollbackToNew {
    implicit lazy val show: Show[RollbackToNew] = Show.show { case RollbackToNew(eventId, projectPath) =>
      s"$eventId, projectPath = $projectPath, status = $New - rollback"
    }
  }

  final case class ToTriplesGenerated(eventId:        CompoundEventId,
                                      projectPath:    projects.Path,
                                      processingTime: EventProcessingTime,
                                      payload:        ZippedEventPayload
  ) extends StatusChangeEvent
  object ToTriplesGenerated {
    implicit lazy val show: Show[ToTriplesGenerated] = Show.show {
      case ToTriplesGenerated(eventId, projectPath, _, _) =>
        s"$eventId, projectPath = $projectPath, status = $TriplesGenerated - update"
    }
  }

  final case class ToFailure[+C <: ProcessingStatus, +N <: FailureStatus](eventId:       CompoundEventId,
                                                                          projectPath:   projects.Path,
                                                                          message:       EventMessage,
                                                                          currentStatus: C,
                                                                          newStatus:     N
  )(implicit evidence:                                                                   AllowedCombination[C, N])
      extends StatusChangeEvent
  object ToFailure {
    implicit lazy val show: Show[ToFailure[ProcessingStatus, FailureStatus]] = Show.show {
      case ToFailure(eventId, projectPath, _, _, newStatus) =>
        s"$eventId, projectPath = $projectPath, status = $newStatus - update"
    }
  }

  sealed trait AllowedCombination[C <: ProcessingStatus, N <: FailureStatus]

  implicit object GenerationToNonRecoverableFailure
      extends AllowedCombination[GeneratingTriples, GenerationNonRecoverableFailure]
  implicit object GenerationToRecoverableFailure
      extends AllowedCombination[GeneratingTriples, GenerationRecoverableFailure]
  implicit object TransformationToNonRecoverableFailure
      extends AllowedCombination[TransformingTriples, TransformationNonRecoverableFailure]
  implicit object TransformationToRecoverableFailure
      extends AllowedCombination[TransformingTriples, TransformationRecoverableFailure]

  final case class RollbackToTriplesGenerated(eventId: CompoundEventId, projectPath: projects.Path)
      extends StatusChangeEvent
  object RollbackToTriplesGenerated {
    implicit lazy val show: Show[RollbackToTriplesGenerated] = Show.show {
      case RollbackToTriplesGenerated(eventId, projectPath) =>
        s"$eventId, projectPath = $projectPath, status = $TriplesGenerated - rollback"
    }
  }

  final case class ToTriplesStore(eventId:        CompoundEventId,
                                  projectPath:    projects.Path,
                                  processingTime: EventProcessingTime
  ) extends StatusChangeEvent
  object ToTriplesStore {
    implicit lazy val show: Show[ToTriplesStore] = Show.show { case ToTriplesStore(eventId, projectPath, _) =>
      s"$eventId, projectPath = $projectPath, status = $TriplesStore - update"
    }
  }

  final case class ToAwaitingDeletion(eventId: CompoundEventId, projectPath: projects.Path) extends StatusChangeEvent
  object ToAwaitingDeletion {
    implicit lazy val show: Show[ToAwaitingDeletion] = Show.show { case ToAwaitingDeletion(eventId, projectPath) =>
      s"$eventId, projectPath = $projectPath, status = $AwaitingDeletion"
    }
  }

  type AllEventsToNew = AllEventsToNew.type
  final case object AllEventsToNew extends StatusChangeEvent {
    implicit lazy val show: Show[AllEventsToNew] = Show.show(_ => s"status = $New")
  }

  implicit def show[E <: StatusChangeEvent](implicit concreteShow: Show[E]): Show[E] = concreteShow
}
