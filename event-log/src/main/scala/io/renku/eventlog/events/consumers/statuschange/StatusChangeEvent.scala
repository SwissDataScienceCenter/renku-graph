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
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax._
import io.renku.events.consumers.Project
import io.renku.graph.model.events._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.projects
import io.renku.tinytypes.json.TinyTypeDecoders._

import java.time.{Duration => JDuration}

sealed trait StatusChangeEvent extends Product {
  def widen: StatusChangeEvent = this

  def subCategoryName: String = productPrefix
}

object StatusChangeEvent {

  final case class ProjectPath(path: projects.Path)
  object ProjectPath {
    implicit val jsonDecoder: Decoder[ProjectPath] = deriveDecoder[ProjectPath]
    implicit val jsonEncoder: Encoder[ProjectPath] = deriveEncoder[ProjectPath]
    implicit val show:        Show[ProjectPath]    = Show.show(_.path.show)
  }

  case object AllEventsToNew extends StatusChangeEvent {
    implicit val show: Show[AllEventsToNew.type] = Show.show { event =>
      show"${event.subCategoryName}"
    }

    implicit val jsonDecoder: Decoder[AllEventsToNew.type] = specificDecoder[AllEventsToNew.type]
    implicit val jsonEncoder: Encoder[AllEventsToNew.type] = specificEncoder[AllEventsToNew.type]
  }

  final case class ProjectEventsToNew(project: Project) extends StatusChangeEvent
  object ProjectEventsToNew {
    implicit lazy val eventType: StatusChangeEventsQueue.EventType[ProjectEventsToNew] =
      StatusChangeEventsQueue.EventType("PROJECT_EVENTS_TO_NEW")

    implicit lazy val show: Show[ProjectEventsToNew] = Show.show { event =>
      show"${event.subCategoryName} ${event.project}"
    }

    implicit val jsonDecoder: Decoder[ProjectEventsToNew] = specificDecoder[ProjectEventsToNew]
    implicit val jsonEncoder: Encoder[ProjectEventsToNew] = specificEncoder[ProjectEventsToNew]
  }

  final case class RedoProjectTransformation(project: ProjectPath) extends StatusChangeEvent
  object RedoProjectTransformation {
    def apply(path: projects.Path): RedoProjectTransformation =
      RedoProjectTransformation(ProjectPath(path))

    implicit lazy val eventType: StatusChangeEventsQueue.EventType[RedoProjectTransformation] =
      StatusChangeEventsQueue.EventType("REDO_PROJECT_TRANSFORMATION")

    implicit lazy val show: Show[RedoProjectTransformation] = Show.show { event =>
      show"${event.subCategoryName} projectPath = ${event.project.path}, status = ${EventStatus.TriplesGenerated}"
    }

    implicit val jsonDecoder: Decoder[RedoProjectTransformation] = specificDecoder[RedoProjectTransformation]
    implicit val jsonEncoder: Encoder[RedoProjectTransformation] = specificEncoder[RedoProjectTransformation]
  }

  final case class RollbackToAwaitingDeletion(project: Project) extends StatusChangeEvent
  object RollbackToAwaitingDeletion {
    implicit val jsonDecoder: Decoder[RollbackToAwaitingDeletion] = specificDecoder[RollbackToAwaitingDeletion]
    implicit val jsonEncoder: Encoder[RollbackToAwaitingDeletion] = specificEncoder[RollbackToAwaitingDeletion]

    implicit lazy val show: Show[RollbackToAwaitingDeletion] = Show.show { event =>
      show"${event.subCategoryName} ${event.project}"
    }
  }

  final case class RollbackToNew(id: EventId, project: Project) extends StatusChangeEvent {
    val eventId: CompoundEventId = CompoundEventId(id, project.id)
  }
  object RollbackToNew {
    implicit val jsonDecoder: Decoder[RollbackToNew] = specificDecoder[RollbackToNew]
    implicit val jsonEncoder: Encoder[RollbackToNew] = specificEncoder[RollbackToNew]

    implicit lazy val show: Show[RollbackToNew] = Show.show { event =>
      show"${event.subCategoryName} id = ${event.id}, ${event.project}, status = $New"
    }
  }

  final case class RollbackToTriplesGenerated(id: EventId, project: Project) extends StatusChangeEvent {
    val eventId: CompoundEventId = CompoundEventId(id, project.id)
  }
  object RollbackToTriplesGenerated {
    implicit val jsonDecoder: Decoder[RollbackToTriplesGenerated] = specificDecoder[RollbackToTriplesGenerated]
    implicit val jsonEncoder: Encoder[RollbackToTriplesGenerated] = specificEncoder[RollbackToTriplesGenerated]

    implicit lazy val show: Show[RollbackToTriplesGenerated] = Show.show { event =>
      show"${event.subCategoryName} id = ${event.id}, ${event.project}"
    }
  }

  final case class ToAwaitingDeletion(id: EventId, project: Project) extends StatusChangeEvent {
    val eventId: CompoundEventId = CompoundEventId(id, project.id)
  }
  object ToAwaitingDeletion {
    implicit val jsonDecoder: Decoder[ToAwaitingDeletion] = specificDecoder[ToAwaitingDeletion]
    implicit val jsonEncoder: Encoder[ToAwaitingDeletion] = specificEncoder[ToAwaitingDeletion]

    implicit lazy val show: Show[ToAwaitingDeletion] = Show.show { event =>
      show"${event.subCategoryName} id = ${event.id}, ${event.project}"
    }
  }

  final case class ToTriplesGenerated(
      id:             EventId,
      project:        Project,
      processingTime: EventProcessingTime,
      payload:        ZippedEventPayload = ZippedEventPayload.empty
  ) extends StatusChangeEvent {
    val eventId: CompoundEventId = CompoundEventId(id, project.id)
  }
  object ToTriplesGenerated {
    implicit val jsonDecoder: Decoder[ToTriplesGenerated] = specificDecoder[ToTriplesGenerated]
    implicit val jsonEncoder: Encoder[ToTriplesGenerated] = specificEncoder[ToTriplesGenerated]

    implicit lazy val show: Show[ToTriplesGenerated] = Show.show { event =>
      show"${event.subCategoryName} id = ${event.id}, ${event.project}"
    }
  }

  final case class ToTriplesStore(
      id:             EventId,
      project:        Project,
      processingTime: EventProcessingTime
  ) extends StatusChangeEvent {
    val eventId: CompoundEventId = CompoundEventId(id, project.id)
  }
  object ToTriplesStore {
    implicit val jsonDecoder: Decoder[ToTriplesStore] = specificDecoder[ToTriplesStore]
    implicit val jsonEncoder: Encoder[ToTriplesStore] = specificEncoder[ToTriplesStore]

    implicit lazy val show: Show[ToTriplesStore] = Show.show { event =>
      show"${event.subCategoryName} id = ${event.id}, ${event.project}"
    }
  }

  final case class ToFailure(
      id:             EventId,
      project:        Project,
      message:        EventMessage,
      newStatus:      FailureStatus,
      executionDelay: Option[JDuration]
  ) extends StatusChangeEvent {
    val eventId: CompoundEventId = CompoundEventId(id, project.id)

    def currentStatus: EventStatus.ProcessingStatus = newStatus match {
      case TransformationRecoverableFailure | TransformationNonRecoverableFailure => TransformingTriples
      case GenerationRecoverableFailure | GenerationNonRecoverableFailure         => GeneratingTriples
    }
  }
  object ToFailure {
    implicit val jsonDecoder: Decoder[ToFailure] = specificDecoder[ToFailure]
    implicit val jsonEncoder: Encoder[ToFailure] = specificEncoder[ToFailure]

    implicit lazy val show: Show[ToFailure] = Show.show { event =>
      show"${event.subCategoryName} id = ${event.id}, ${event.project}, status = ${event.newStatus}"
    }
  }

  private object JsonCodec {
    implicit val decodingConfig: Configuration =
      Configuration.default.withDiscriminator("subCategory").withDefaults

    private implicit val dummyPayloadDecoder: Decoder[ZippedEventPayload] =
      Decoder.instance(_ => Right(ZippedEventPayload.empty))
    private implicit val dummyPayloadEncoder: Encoder[ZippedEventPayload] =
      Encoder.instance(_ => Json.Null)

    implicit val toFailureJsonDecoder: Decoder[ToFailure] = deriveDecoder[ToFailure]
    implicit val toFailureJsonEncoder: Encoder[ToFailure] = deriveEncoder[ToFailure]

    implicit val toTriplesStoreJsonDecoder: Decoder[ToTriplesStore] = deriveDecoder[ToTriplesStore]
    implicit val toTriplesStoreJsonEncoder: Encoder[ToTriplesStore] = deriveEncoder[ToTriplesStore]

    private case class PartialTriplesGenerated(id: EventId, project: Project, processingTime: EventProcessingTime)
    implicit val toTriplesGeneratedJsonDecoder: Decoder[ToTriplesGenerated] =
      deriveDecoder[PartialTriplesGenerated].map { case PartialTriplesGenerated(id, project, time) =>
        ToTriplesGenerated(id, project, time, ZippedEventPayload.empty)
      }

    implicit val toTriplesGeneratedJsonEncoder: Encoder[ToTriplesGenerated] = deriveEncoder[ToTriplesGenerated]

    implicit val toAwaitingDeletionJsonDecoder: Decoder[ToAwaitingDeletion] = deriveDecoder[ToAwaitingDeletion]
    implicit val toAwaitingDeletionJsonEncoder: Encoder[ToAwaitingDeletion] = deriveEncoder[ToAwaitingDeletion]

    implicit val rollbackToTriplesGeneratedJsonDecoder: Decoder[RollbackToTriplesGenerated] =
      deriveDecoder[RollbackToTriplesGenerated]
    implicit val rollbackToTriplesGeneratedJsonEncoder: Encoder[RollbackToTriplesGenerated] =
      deriveEncoder[RollbackToTriplesGenerated]

    implicit val rollbackToNewJsonDecoder: Decoder[RollbackToNew] = deriveDecoder[RollbackToNew]
    implicit val rollbackToNewJsonEncoder: Encoder[RollbackToNew] = deriveEncoder[RollbackToNew]

    implicit val rollbackToAwaitingDeletionJsonDecoder: Decoder[RollbackToAwaitingDeletion] =
      deriveDecoder[RollbackToAwaitingDeletion]
    implicit val rollbackToAwaitingDeletionJsonEncoder: Encoder[RollbackToAwaitingDeletion] =
      deriveEncoder[RollbackToAwaitingDeletion]

    implicit val redoProjectTransformationJsonDecoder: Decoder[RedoProjectTransformation] =
      deriveDecoder[RedoProjectTransformation]
    implicit val redoProjectTransformationJsonEncoder: Encoder[RedoProjectTransformation] =
      deriveEncoder[RedoProjectTransformation]

    implicit val projectEventsToNewJsonDecoder: Decoder[ProjectEventsToNew] = deriveDecoder[ProjectEventsToNew]
    implicit val projectEventsToNewJsonEncoder: Encoder[ProjectEventsToNew] = deriveEncoder[ProjectEventsToNew]

    implicit val allEventsToNewJsonDecoder: Decoder[AllEventsToNew.type] = deriveDecoder[AllEventsToNew.type]
    implicit val allEventsToNewJsonEncoder: Encoder[AllEventsToNew.type] = deriveEncoder[AllEventsToNew.type]

    val jsonDecoder: Decoder[StatusChangeEvent] = deriveConfiguredDecoder[StatusChangeEvent]
    val jsonEncoder: Encoder[StatusChangeEvent] = deriveConfiguredEncoder[StatusChangeEvent].mapJsonObject { obj =>
      // not a deep remove here, since it's not necessary
      obj.filter(_._2 != Json.Null).add("categoryName", "EVENTS_STATUS_CHANGE".asJson)
    }
  }

  implicit val jsonDecoder: Decoder[StatusChangeEvent] = JsonCodec.jsonDecoder
  implicit val jsonEncoder: Encoder[StatusChangeEvent] = JsonCodec.jsonEncoder

  implicit def show[E <: StatusChangeEvent](implicit concreteShow: Show[E]): Show[E] = concreteShow

  def show: Show[StatusChangeEvent] = {
    import io.renku.data.CoproductShow._

    implicitly[Show[StatusChangeEvent]]
  }

  private def specificDecoder[A <: StatusChangeEvent]: Decoder[A] =
    JsonCodec.jsonDecoder.emap(ev => ev.asInstanceOf[A].asRight)

  private def specificEncoder[A <: StatusChangeEvent]: Encoder[A] =
    JsonCodec.jsonEncoder.contramap(_.widen)
}
