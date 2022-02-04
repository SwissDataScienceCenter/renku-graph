/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import cats.implicits.showInterpolator
import io.circe.literal._
import io.circe.{Decoder, Encoder}
import io.renku.eventlog.EventMessage
import io.renku.events.consumers.Project
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{CompoundEventId, EventProcessingTime, ZippedEventPayload}
import io.renku.graph.model.projects
import io.renku.tinytypes.json.TinyTypeDecoders._
import java.time.Duration

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

  final case class ToFailure[+C <: ProcessingStatus, +N <: FailureStatus](eventId:             CompoundEventId,
                                                                          projectPath:         projects.Path,
                                                                          message:             EventMessage,
                                                                          currentStatus:       C,
                                                                          newStatus:           N,
                                                                          maybeExecutionDelay: Option[Duration]
  )(implicit evidence:                                                                         AllowedCombination[C, N])
      extends StatusChangeEvent
  object ToFailure {
    implicit lazy val show: Show[ToFailure[ProcessingStatus, FailureStatus]] = Show.show {
      case ToFailure(eventId, projectPath, _, _, newStatus, _) =>
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

  final case class RollbackToAwaitingDeletion(project: Project) extends StatusChangeEvent
  object RollbackToAwaitingDeletion {
    implicit lazy val show: Show[RollbackToAwaitingDeletion] = Show.show {
      case RollbackToAwaitingDeletion(Project(id, path)) =>
        s"project_id = $id, projectPath = $path, status = $AwaitingDeletion - rollback"
    }
  }

  type AllEventsToNew = AllEventsToNew.type
  final case object AllEventsToNew extends StatusChangeEvent {
    implicit lazy val show: Show[AllEventsToNew] = Show.show(_ => s"status = $New")
  }

  final case class ProjectEventsToNew(project: Project) extends StatusChangeEvent
  object ProjectEventsToNew {
    implicit lazy val show: Show[ProjectEventsToNew] = Show.show { case ProjectEventsToNew(project) =>
      show"$project, status = $New"
    }

    implicit lazy val encoder: Encoder[ProjectEventsToNew] = Encoder.instance {
      case ProjectEventsToNew(Project(id, path)) => json"""{
        "project": {
          "id": ${id.value},
          "path": ${path.value}
        }    
      }"""
    }
    implicit lazy val decoder: Decoder[ProjectEventsToNew] = cursor =>
      for {
        id   <- cursor.downField("project").downField("id").as[projects.Id]
        path <- cursor.downField("project").downField("path").as[projects.Path]
      } yield ProjectEventsToNew(Project(id, path))

    implicit lazy val eventType: StatusChangeEventsQueue.EventType[ProjectEventsToNew] =
      StatusChangeEventsQueue.EventType("PROJECT_EVENTS_TO_NEW")
  }

  implicit def show[E <: StatusChangeEvent](implicit concreteShow: Show[E]): Show[E] = concreteShow
}
