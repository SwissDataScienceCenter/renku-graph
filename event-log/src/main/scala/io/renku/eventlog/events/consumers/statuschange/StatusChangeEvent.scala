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

package io.renku.eventlog.events.consumers.statuschange

import cats.Show
import cats.syntax.all._
import io.circe.{Decoder, Encoder, Json}
import io.circe.generic.extras.Configuration
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.renku.events.consumers.Project
import io.renku.graph.model.events._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.projects
import io.renku.tinytypes.json.TinyTypeDecoders._

import java.time.{Duration => JDuration}

sealed trait StatusChangeEvent extends Product {
  val silent: Boolean

  def widen: StatusChangeEvent = this
}

object StatusChangeEvent {
  implicit def show[E <: StatusChangeEvent](implicit concreteShow: Show[E]): Show[E] = concreteShow

  implicit val decodingConfig: Configuration =
    Configuration.default.withDiscriminator("subCategory").withDefaults

  final case class ProjectPath(path: projects.Path)
  object ProjectPath {
    implicit val jsonDecoder: Decoder[ProjectPath] = deriveDecoder[ProjectPath]
    implicit val jsonEncoder: Encoder[ProjectPath] = deriveEncoder[ProjectPath]
  }

  case object AllEventsToNew extends StatusChangeEvent {
    val silent = false

    implicit val show: Show[AllEventsToNew.type] = Show.fromToString

    implicit val jsonDecoder: Decoder[AllEventsToNew.type] = deriveDecoder[AllEventsToNew.type]
    implicit val jsonEncoder: Encoder[AllEventsToNew.type] = deriveEncoder[AllEventsToNew.type]
  }

  final case class ProjectEventsToNew(project: Project) extends StatusChangeEvent {
    override val silent: Boolean = false
  }
  object ProjectEventsToNew {
    implicit lazy val eventType: StatusChangeEventsQueue.EventType[ProjectEventsToNew] =
      StatusChangeEventsQueue.EventType("PROJECT_EVENTS_TO_NEW")

    implicit lazy val show: Show[ProjectEventsToNew] = Show.show { case ProjectEventsToNew(project) =>
      show"$project, status = $New"
    }
    implicit val jsonDecoder: Decoder[ProjectEventsToNew] = deriveDecoder[ProjectEventsToNew]
    implicit val jsonEncoder: Encoder[ProjectEventsToNew] = deriveEncoder[ProjectEventsToNew]
  }

  final case class RedoProjectTransformation(project: ProjectPath) extends StatusChangeEvent {
    override val silent: Boolean = false
  }
  object RedoProjectTransformation {
    implicit lazy val eventType: StatusChangeEventsQueue.EventType[RedoProjectTransformation] =
      StatusChangeEventsQueue.EventType("REDO_PROJECT_TRANSFORMATION")

    implicit lazy val show: Show[RedoProjectTransformation] = Show.show { case RedoProjectTransformation(projectPath) =>
      s"projectPath = $projectPath, status = $ToTriplesGenerated - redo"
    }

    implicit val jsonDecoder: Decoder[RedoProjectTransformation] = deriveDecoder[RedoProjectTransformation]
    implicit val jsonEncoder: Encoder[RedoProjectTransformation] = deriveEncoder[RedoProjectTransformation]
  }

  final case class RollbackToAwaitingDeletion(project: Project) extends StatusChangeEvent {
    override val silent: Boolean = true
  }
  object RollbackToAwaitingDeletion {
    implicit val jsonDecoder: Decoder[RollbackToAwaitingDeletion] = deriveDecoder[RollbackToAwaitingDeletion]
    implicit val jsonEncoder: Encoder[RollbackToAwaitingDeletion] = deriveEncoder[RollbackToAwaitingDeletion]
  }

  final case class RollbackToNew(id: EventId, project: Project) extends StatusChangeEvent {
    override val silent: Boolean         = true
    val eventId:         CompoundEventId = CompoundEventId(id, project.id)
  }
  object RollbackToNew {
    implicit val jsonDecoder: Decoder[RollbackToNew] = deriveDecoder[RollbackToNew]
    implicit val jsonEncoder: Encoder[RollbackToNew] = deriveEncoder[RollbackToNew]
  }

  final case class RollbackToTriplesGenerated(id: EventId, project: Project) extends StatusChangeEvent {
    override val silent: Boolean         = true
    val eventId:         CompoundEventId = CompoundEventId(id, project.id)
  }
  object RollbackToTriplesGenerated {
    implicit val jsonDecoder: Decoder[RollbackToTriplesGenerated] = deriveDecoder[RollbackToTriplesGenerated]
    implicit val jsonEncoder: Encoder[RollbackToTriplesGenerated] = deriveEncoder[RollbackToTriplesGenerated]
  }

  final case class ToAwaitingDeletion(id: EventId, project: Project) extends StatusChangeEvent {
    override val silent: Boolean         = false
    val eventId:         CompoundEventId = CompoundEventId(id, project.id)
  }
  object ToAwaitingDeletion {
    implicit val jsonDecoder: Decoder[ToAwaitingDeletion] = deriveDecoder[ToAwaitingDeletion]
    implicit val jsonEncoder: Encoder[ToAwaitingDeletion] = deriveEncoder[ToAwaitingDeletion]
  }

  final case class ToTriplesGenerated(id:             EventId,
                                      project:        Project,
                                      processingTime: EventProcessingTime,
                                      payload:        ZippedEventPayload = ZippedEventPayload(new Array[Byte](0))
  ) extends StatusChangeEvent {
    override val silent: Boolean         = false
    val eventId:         CompoundEventId = CompoundEventId(id, project.id)
  }
  object ToTriplesGenerated {
    private implicit val dummyPayloadDecoder: Decoder[ZippedEventPayload] =
      Decoder.instance(_ => Right(ZippedEventPayload(new Array[Byte](0))))
    private implicit val dummyPayloadEncoder: Encoder[ZippedEventPayload] =
      Encoder.instance(_ => Json.Null)

    implicit val jsonDecoder: Decoder[ToTriplesGenerated] = deriveDecoder[ToTriplesGenerated]
    implicit val jsonEncoder: Encoder[ToTriplesGenerated] = deriveEncoder[ToTriplesGenerated]
  }

  final case class ToTriplesStore(
      id:             EventId,
      project:        Project,
      processingTime: EventProcessingTime
  ) extends StatusChangeEvent {
    override val silent: Boolean         = false
    val eventId:         CompoundEventId = CompoundEventId(id, project.id)
  }
  object ToTriplesStore {
    implicit val jsonDecoder: Decoder[ToTriplesStore] = deriveDecoder[ToTriplesStore]
    implicit val jsonEncoder: Encoder[ToTriplesStore] = deriveEncoder[ToTriplesStore]
  }

  final case class ToFailure(
      id:                  EventId,
      project:             Project,
      message:             EventMessage,
      newStatus:           FailureStatus,
      maybeExecutionDelay: Option[JDuration]
  ) extends StatusChangeEvent {
    override val silent: Boolean = false

    val eventId: CompoundEventId = CompoundEventId(id, project.id)

    def currentStatus: EventStatus.ProcessingStatus = newStatus match {
      case TransformationRecoverableFailure | TransformationNonRecoverableFailure => TransformingTriples
      case GenerationRecoverableFailure | GenerationNonRecoverableFailure         => GeneratingTriples
    }
  }
  object ToFailure {
    implicit val jsonDecoder: Decoder[ToFailure] = deriveDecoder[ToFailure]
    implicit val jsonEncoder: Encoder[ToFailure] = deriveEncoder[ToFailure]
  }

  implicit val jsonDecoder: Decoder[StatusChangeEvent] = deriveDecoder[StatusChangeEvent]
  implicit val jsonEncoder: Encoder[StatusChangeEvent] = deriveEncoder[StatusChangeEvent]
}
