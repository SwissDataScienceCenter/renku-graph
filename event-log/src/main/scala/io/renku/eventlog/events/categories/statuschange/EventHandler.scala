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

import cats.data.EitherT
import cats.effect.{Async, Spawn}
import cats.syntax.all._
import cats.{MonadThrow, Show}
import io.circe.DecodingFailure
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.eventlog.events.categories.statuschange.DBUpdater.EventUpdaterFactory
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent._
import io.renku.eventlog.{EventLogDB, EventMessage}
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest, UnsupportedEventType}
import io.renku.events.consumers._
import io.renku.events.{EventRequestContent, consumers}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{CategoryName, CompoundEventId, EventId, EventProcessingTime, EventStatus, ZippedEventPayload}
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram}
import org.typelevel.log4cats.Logger

import java.time.{Duration => JDuration}
import scala.util.control.NonFatal

private class EventHandler[F[_]: Async: Logger](
    override val categoryName: CategoryName,
    eventsQueue:               StatusChangeEventsQueue[F],
    statusChanger:             StatusChanger[F],
    deliveryInfoRemover:       DeliveryInfoRemover[F],
    queriesExecTimes:          LabeledHistogram[F, SqlStatement.Name]
) extends consumers.EventHandlerWithProcessLimiter[F](ConcurrentProcessesLimiter.withoutLimit) {

  import EventHandler._

  override def createHandlingProcess(
      request: EventRequestContent
  ): F[EventHandlingProcess[F]] = EventHandlingProcess[F] {
    tryHandle(
      requestAs[ToTriplesGenerated],
      requestAs[ToTriplesStore],
      requestAs[ToFailure[ProcessingStatus, FailureStatus]],
      requestAs[RollbackToNew],
      requestAs[RollbackToTriplesGenerated],
      requestAs[ToAwaitingDeletion],
      requestAs[RollbackToAwaitingDeletion],
      requestAs[ProjectEventsToNew],
      requestAs[AllEventsToNew]
    )(request)
  }

  private def tryHandle(
      options: EventRequestContent => EitherT[F, EventSchedulingResult, Accepted]*
  ): EventRequestContent => EitherT[F, EventSchedulingResult, Accepted] = request =>
    options.foldLeft(
      EitherT.left[Accepted](UnsupportedEventType.pure[F].widen[EventSchedulingResult])
    ) { case (previousOptionResult, option) => previousOptionResult orElse option(request) }

  private def requestAs[E <: StatusChangeEvent](request: EventRequestContent)(implicit
      updaterFactory:                                    EventUpdaterFactory[F, E],
      show:                                              Show[E],
      decoder:                                           EventRequestContent => Either[DecodingFailure, E]
  ): EitherT[F, EventSchedulingResult, Accepted] = EitherT(
    decode[E](request)
      .map(startUpdate)
      .leftMap(_ => BadRequest)
      .leftWiden[EventSchedulingResult]
      .sequence
  )

  private def startUpdate[E <: StatusChangeEvent](implicit
      updaterFactory: EventUpdaterFactory[F, E],
      show:           Show[E]
  ): E => F[Accepted] = event =>
    Spawn[F]
      .start(executeUpdate(event))
      .map(_ => Accepted)
      .flatTap(Logger[F].log(event.show))

  private def executeUpdate[E <: StatusChangeEvent](
      event:                 E
  )(implicit updaterFactory: EventUpdaterFactory[F, E], show: Show[E]) = (for {
    factory <- updaterFactory(eventsQueue, deliveryInfoRemover, queriesExecTimes)
    result <- statusChanger
                .updateStatuses(event)(factory)
                .flatTap(_ => Logger[F].logInfo(event, "Processed"))
  } yield result) recoverWith { case NonFatal(e) => Logger[F].logError(event, e) >> e.raiseError[F, Unit] }
}

private object EventHandler {

  def apply[F[_]: Async: Logger](
      sessionResource:                    SessionResource[F, EventLogDB],
      eventsQueue:                        StatusChangeEventsQueue[F],
      queriesExecTimes:                   LabeledHistogram[F, SqlStatement.Name],
      awaitingTriplesGenerationGauge:     LabeledGauge[F, projects.Path],
      underTriplesGenerationGauge:        LabeledGauge[F, projects.Path],
      awaitingTriplesTransformationGauge: LabeledGauge[F, projects.Path],
      underTriplesTransformationGauge:    LabeledGauge[F, projects.Path]
  ): F[EventHandler[F]] = for {
    deliveryInfoRemover <- DeliveryInfoRemover(queriesExecTimes)
    gaugesUpdater <- MonadThrow[F].catchNonFatal(
                       new GaugesUpdaterImpl[F](awaitingTriplesGenerationGauge,
                                                awaitingTriplesTransformationGauge,
                                                underTriplesTransformationGauge,
                                                underTriplesGenerationGauge
                       )
                     )
    statusChanger <- MonadThrow[F].catchNonFatal(new StatusChangerImpl[F](sessionResource, gaugesUpdater))
    _             <- registerHandlers(eventsQueue, statusChanger, queriesExecTimes)
  } yield new EventHandler[F](categoryName, eventsQueue, statusChanger, deliveryInfoRemover, queriesExecTimes)

  private def registerHandlers[F[_]: Async: Logger](eventsQueue: StatusChangeEventsQueue[F],
                                                    statusChanger:    StatusChanger[F],
                                                    queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ) = for {
    projectsToNewUpdater <- ProjectEventsToNewUpdater(queriesExecTimes)
    _ <- eventsQueue.register[ProjectEventsToNew](statusChanger.updateStatuses(_)(projectsToNewUpdater))
  } yield ()

  import io.renku.tinytypes.json.TinyTypeDecoders._

  private def decode[E <: StatusChangeEvent](
      request:        EventRequestContent
  )(implicit decoder: EventRequestContent => Either[DecodingFailure, E]) = decoder(request)

  private implicit lazy val eventTriplesGeneratedDecoder
      : EventRequestContent => Either[DecodingFailure, ToTriplesGenerated] = {
    case EventRequestContent.WithPayload(event, payload: ZippedEventPayload) =>
      for {
        id             <- event.hcursor.downField("id").as[EventId]
        projectId      <- event.hcursor.downField("project").downField("id").as[projects.Id]
        projectPath    <- event.hcursor.downField("project").downField("path").as[projects.Path]
        processingTime <- event.hcursor.downField("processingTime").as[EventProcessingTime]
        _ <- event.hcursor.downField("newStatus").as[EventStatus].flatMap {
               case TriplesGenerated => Right(())
               case status           => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield ToTriplesGenerated(CompoundEventId(id, projectId), projectPath, processingTime, payload)
    case _ => Left(DecodingFailure("Missing event payload", Nil))
  }

  private implicit lazy val eventTripleStoreDecoder: EventRequestContent => Either[DecodingFailure, ToTriplesStore] = {
    request =>
      for {
        id             <- request.event.hcursor.downField("id").as[EventId]
        projectId      <- request.event.hcursor.downField("project").downField("id").as[projects.Id]
        projectPath    <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
        processingTime <- request.event.hcursor.downField("processingTime").as[EventProcessingTime]
        _ <- request.event.hcursor.downField("newStatus").as[EventStatus].flatMap {
               case TriplesStore => Right(())
               case status       => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield ToTriplesStore(CompoundEventId(id, projectId), projectPath, processingTime)
  }

  private implicit lazy val eventFailureDecoder
      : EventRequestContent => Either[DecodingFailure, ToFailure[ProcessingStatus, FailureStatus]] = { request =>
    for {
      id          <- request.event.hcursor.downField("id").as[EventId]
      projectId   <- request.event.hcursor.downField("project").downField("id").as[projects.Id]
      projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
      message     <- request.event.hcursor.downField("message").as[EventMessage]
      eventId = CompoundEventId(id, projectId)
      executionDelay <-
        request.event.hcursor.downField("executionDelay").as[Option[JDuration]]
      statusChangeEvent <- request.event.hcursor.downField("newStatus").as[EventStatus].flatMap {
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

  private implicit lazy val eventRollbackToNewDecoder: EventRequestContent => Either[DecodingFailure, RollbackToNew] = {
    request =>
      for {
        id          <- request.event.hcursor.downField("id").as[EventId]
        projectId   <- request.event.hcursor.downField("project").downField("id").as[projects.Id]
        projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
        _ <- request.event.hcursor.downField("newStatus").as[EventStatus].flatMap {
               case New    => Right(())
               case status => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield RollbackToNew(CompoundEventId(id, projectId), projectPath)
  }

  private implicit lazy val eventRollbackToTriplesGeneratedDecoder
      : EventRequestContent => Either[DecodingFailure, RollbackToTriplesGenerated] = { request =>
    for {
      id          <- request.event.hcursor.downField("id").as[EventId]
      projectId   <- request.event.hcursor.downField("project").downField("id").as[projects.Id]
      projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
      _ <- request.event.hcursor.downField("newStatus").as[EventStatus].flatMap {
             case TriplesGenerated => Right(())
             case status           => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
           }
    } yield RollbackToTriplesGenerated(CompoundEventId(id, projectId), projectPath)
  }

  private implicit lazy val eventToAwaitingDeletionDecoder
      : EventRequestContent => Either[DecodingFailure, ToAwaitingDeletion] = { request =>
    for {
      id          <- request.event.hcursor.downField("id").as[EventId]
      projectId   <- request.event.hcursor.downField("project").downField("id").as[projects.Id]
      projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
      _ <- request.event.hcursor.downField("newStatus").as[EventStatus].flatMap {
             case AwaitingDeletion => Right(())
             case status           => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
           }
    } yield ToAwaitingDeletion(CompoundEventId(id, projectId), projectPath)
  }

  private implicit lazy val eventRollbackToAwaitingDeletionDecoder
      : EventRequestContent => Either[DecodingFailure, RollbackToAwaitingDeletion] = { request =>
    for {
      projectId   <- request.event.hcursor.downField("project").downField("id").as[projects.Id]
      projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
      _ <- request.event.hcursor.downField("newStatus").as[EventStatus].flatMap {
             case AwaitingDeletion => Right(())
             case status           => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
           }
    } yield RollbackToAwaitingDeletion(Project(projectId, projectPath))
  }

  private implicit lazy val eventToProjectEventToNewDecoder
      : EventRequestContent => Either[DecodingFailure, ProjectEventsToNew] = { request =>
    for {
      projectId   <- request.event.hcursor.downField("project").downField("id").as[projects.Id]
      projectPath <- request.event.hcursor.downField("project").downField("path").as[projects.Path]
      _ <- request.event.hcursor.downField("newStatus").as[EventStatus].flatMap {
             case New    => Right(())
             case status => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
           }
    } yield ProjectEventsToNew(Project(projectId, projectPath))
  }

  private implicit lazy val allEventNewDecoder: EventRequestContent => Either[DecodingFailure, AllEventsToNew] =
    _.event.hcursor.downField("newStatus").as[EventStatus] >>= {
      case New    => Right(AllEventsToNew)
      case status => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
    }
}
