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
import cats.implicits.showInterpolator
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure, Encoder}
import io.circe.literal._
import io.renku.events.consumers.Project
import io.renku.events.EventRequestContent
import io.renku.graph.model.{events, projects}
import io.renku.graph.model.events.{CompoundEventId, EventMessage, EventProcessingTime, ZippedEventPayload}
import io.renku.graph.model.events.EventStatus._
import io.renku.tinytypes.json.TinyTypeDecoders._

import java.time.Duration

private sealed trait StatusChangeEvent extends Product with Serializable {
  val silent: Boolean
}

private object StatusChangeEvent {

  final case class RollbackToNew(eventId: CompoundEventId, projectPath: projects.Path) extends StatusChangeEvent {
    override val silent: Boolean = true
  }
  object RollbackToNew {

    val decoder: EventRequestContent => Either[DecodingFailure, RollbackToNew] = { request =>
      for {
        id          <- request.event.hcursor.downField("id").as[events.EventId]
        projectId   <- request.event.hcursor.downField("project").downField("id").as[projects.GitLabId]
        projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
        _ <- request.event.hcursor.downField("newStatus").as[events.EventStatus] >>= {
               case New    => Right(())
               case status => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield RollbackToNew(CompoundEventId(id, projectId), projectPath)
    }

    implicit lazy val show: Show[RollbackToNew] = Show.show { case RollbackToNew(eventId, projectPath) =>
      s"$eventId, projectPath = $projectPath, status = $New - rollback"
    }
  }

  final case class ToTriplesGenerated(eventId:        CompoundEventId,
                                      projectPath:    projects.Path,
                                      processingTime: EventProcessingTime,
                                      payload:        ZippedEventPayload
  ) extends StatusChangeEvent {
    override val silent: Boolean = false
  }
  object ToTriplesGenerated {

    val decoder: EventRequestContent => Either[DecodingFailure, ToTriplesGenerated] = {
      case EventRequestContent.WithPayload(event, payload: ZippedEventPayload) =>
        for {
          id             <- event.hcursor.downField("id").as[events.EventId]
          projectId      <- event.hcursor.downField("project").downField("id").as[projects.GitLabId]
          projectPath    <- event.hcursor.downField("project").downField("path").as[projects.Path]
          processingTime <- event.hcursor.downField("processingTime").as[EventProcessingTime]
          _ <- event.hcursor.downField("newStatus").as[events.EventStatus].flatMap {
                 case TriplesGenerated => Right(())
                 case status           => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
               }
        } yield ToTriplesGenerated(CompoundEventId(id, projectId), projectPath, processingTime, payload)
      case _ => Left(DecodingFailure("Missing event payload", Nil))
    }

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
  )(implicit evidence: AllowedCombination[C, N])
      extends StatusChangeEvent {
    override val silent: Boolean = false
  }
  object ToFailure {
    import java.time.{Duration => JDuration}

    val decoder: EventRequestContent => Either[DecodingFailure, ToFailure[ProcessingStatus, FailureStatus]] = {
      request =>
        for {
          id          <- request.event.hcursor.downField("id").as[events.EventId]
          projectId   <- request.event.hcursor.downField("project").downField("id").as[projects.GitLabId]
          projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
          message     <- request.event.hcursor.downField("message").as[EventMessage]
          eventId = CompoundEventId(id, projectId)
          executionDelay <-
            request.event.hcursor.downField("executionDelay").as[Option[JDuration]]
          statusChangeEvent <- request.event.hcursor.downField("newStatus").as[events.EventStatus].flatMap {
                                 case status: GenerationRecoverableFailure =>
                                   ToFailure(eventId,
                                             projectPath,
                                             message,
                                             GeneratingTriples,
                                             status,
                                             executionDelay
                                   ).asRight
                                 case status: GenerationNonRecoverableFailure =>
                                   ToFailure(eventId,
                                             projectPath,
                                             message,
                                             GeneratingTriples,
                                             status,
                                             maybeExecutionDelay = None
                                   ).asRight
                                 case status: TransformationRecoverableFailure =>
                                   ToFailure(eventId,
                                             projectPath,
                                             message,
                                             TransformingTriples,
                                             status,
                                             executionDelay
                                   ).asRight
                                 case status: TransformationNonRecoverableFailure =>
                                   ToFailure(eventId,
                                             projectPath,
                                             message,
                                             TransformingTriples,
                                             status,
                                             maybeExecutionDelay = None
                                   ).asRight
                                 case status =>
                                   DecodingFailure(s"Unrecognized event status $status", Nil)
                                     .asLeft[ToFailure[ProcessingStatus, FailureStatus]]
                               }
        } yield statusChangeEvent
    }

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
      extends StatusChangeEvent {
    override val silent: Boolean = true
  }
  object RollbackToTriplesGenerated {

    val decoder: EventRequestContent => Either[DecodingFailure, RollbackToTriplesGenerated] = { request =>
      for {
        id          <- request.event.hcursor.downField("id").as[events.EventId]
        projectId   <- request.event.hcursor.downField("project").downField("id").as[projects.GitLabId]
        projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
        _ <- request.event.hcursor.downField("newStatus").as[events.EventStatus].flatMap {
               case TriplesGenerated => Right(())
               case status           => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield RollbackToTriplesGenerated(CompoundEventId(id, projectId), projectPath)
    }

    implicit lazy val show: Show[RollbackToTriplesGenerated] = Show.show {
      case RollbackToTriplesGenerated(eventId, projectPath) =>
        s"$eventId, projectPath = $projectPath, status = $TriplesGenerated - rollback"
    }
  }

  final case class ToTriplesStore(eventId:        CompoundEventId,
                                  projectPath:    projects.Path,
                                  processingTime: EventProcessingTime
  ) extends StatusChangeEvent {
    override val silent: Boolean = false
  }
  object ToTriplesStore {

    val decoder: EventRequestContent => Either[DecodingFailure, ToTriplesStore] = { request =>
      for {
        id             <- request.event.hcursor.downField("id").as[events.EventId]
        projectId      <- request.event.hcursor.downField("project").downField("id").as[projects.GitLabId]
        projectPath    <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
        processingTime <- request.event.hcursor.downField("processingTime").as[EventProcessingTime]
        _ <- request.event.hcursor.downField("newStatus").as[events.EventStatus].flatMap {
               case TriplesStore => Right(())
               case status       => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield ToTriplesStore(CompoundEventId(id, projectId), projectPath, processingTime)
    }

    implicit lazy val show: Show[ToTriplesStore] = Show.show { case ToTriplesStore(eventId, projectPath, _) =>
      s"$eventId, projectPath = $projectPath, status = $TriplesStore - update"
    }
  }

  final case class ToAwaitingDeletion(eventId: CompoundEventId, projectPath: projects.Path) extends StatusChangeEvent {
    override val silent: Boolean = false
  }
  object ToAwaitingDeletion {

    val decoder: EventRequestContent => Either[DecodingFailure, ToAwaitingDeletion] = { request =>
      for {
        id          <- request.event.hcursor.downField("id").as[events.EventId]
        projectId   <- request.event.hcursor.downField("project").downField("id").as[projects.GitLabId]
        projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
        _ <- request.event.hcursor.downField("newStatus").as[events.EventStatus].flatMap {
               case AwaitingDeletion => Right(())
               case status           => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield ToAwaitingDeletion(CompoundEventId(id, projectId), projectPath)
    }

    implicit lazy val show: Show[ToAwaitingDeletion] = Show.show { case ToAwaitingDeletion(eventId, projectPath) =>
      s"$eventId, projectPath = $projectPath, status = $AwaitingDeletion"
    }
  }

  final case class RollbackToAwaitingDeletion(project: Project) extends StatusChangeEvent {
    override val silent: Boolean = true
  }
  object RollbackToAwaitingDeletion {

    val decoder: EventRequestContent => Either[DecodingFailure, RollbackToAwaitingDeletion] = { request =>
      for {
        projectId   <- request.event.hcursor.downField("project").downField("id").as[projects.GitLabId]
        projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
        _ <- request.event.hcursor.downField("newStatus").as[events.EventStatus].flatMap {
               case AwaitingDeletion => Right(())
               case status           => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield RollbackToAwaitingDeletion(Project(projectId, projectPath))
    }

    implicit lazy val show: Show[RollbackToAwaitingDeletion] = Show.show {
      case RollbackToAwaitingDeletion(Project(id, path)) =>
        s"project_id = $id, projectPath = $path, status = $AwaitingDeletion - rollback"
    }
  }

  final case class RedoProjectTransformation(projectPath: projects.Path) extends StatusChangeEvent {
    override val silent: Boolean = false
  }

  object RedoProjectTransformation {

    val decoder: EventRequestContent => Either[DecodingFailure, RedoProjectTransformation] = { request =>
      for {
        projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
        _ <- request.event.hcursor.downField("newStatus").as[events.EventStatus] >>= {
               case TriplesGenerated => Right(())
               case status           => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield RedoProjectTransformation(projectPath)
    }

    implicit lazy val encoder: Encoder[RedoProjectTransformation] = Encoder.instance {
      case RedoProjectTransformation(path) => json"""{
        "project": {
          "path": ${path.value}
        }
      }"""
    }

    implicit lazy val eventDecoder: Decoder[RedoProjectTransformation] =
      _.downField("project").downField("path").as[projects.Path].map(RedoProjectTransformation(_))

    implicit lazy val eventType: StatusChangeEventsQueue.EventType[RedoProjectTransformation] =
      StatusChangeEventsQueue.EventType("REDO_PROJECT_TRANSFORMATION")

    implicit lazy val show: Show[RedoProjectTransformation] = Show.show { case RedoProjectTransformation(projectPath) =>
      s"projectPath = $projectPath, status = $TriplesGenerated - redo"
    }
  }

  final case class ProjectEventsToNew(project: Project) extends StatusChangeEvent {
    override val silent: Boolean = false
  }
  object ProjectEventsToNew {

    val decoder: EventRequestContent => Either[DecodingFailure, ProjectEventsToNew] = { request =>
      for {
        projectId   <- request.event.hcursor.downField("project").downField("id").as[projects.GitLabId]
        projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
        _ <- request.event.hcursor.downField("newStatus").as[events.EventStatus].flatMap {
               case New    => Right(())
               case status => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield ProjectEventsToNew(Project(projectId, projectPath))
    }

    implicit lazy val encoder: Encoder[ProjectEventsToNew] = Encoder.instance {
      case ProjectEventsToNew(Project(id, path)) => json"""{
        "project": {
          "id":   ${id.value},
          "path": ${path.value}
        }
      }"""
    }
    implicit lazy val eventDecoder: Decoder[ProjectEventsToNew] = cursor =>
      for {
        id   <- cursor.downField("project").downField("id").as[projects.GitLabId]
        path <- cursor.downField("project").downField("path").as[projects.Path]
      } yield ProjectEventsToNew(Project(id, path))

    implicit lazy val eventType: StatusChangeEventsQueue.EventType[ProjectEventsToNew] =
      StatusChangeEventsQueue.EventType("PROJECT_EVENTS_TO_NEW")

    implicit lazy val show: Show[ProjectEventsToNew] = Show.show { case ProjectEventsToNew(project) =>
      show"$project, status = $New"
    }
  }

  type AllEventsToNew = AllEventsToNew.type
  final case object AllEventsToNew extends StatusChangeEvent {
    override val silent: Boolean = false

    val decoder: EventRequestContent => Either[DecodingFailure, AllEventsToNew] =
      _.event.hcursor.downField("newStatus").as[events.EventStatus] >>= {
        case New    => Right(AllEventsToNew)
        case status => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
      }

    implicit lazy val show: Show[AllEventsToNew] = Show.show(_ => s"status = $New")
  }

  implicit def show[E <: StatusChangeEvent](implicit concreteShow: Show[E]): Show[E] = concreteShow
}
