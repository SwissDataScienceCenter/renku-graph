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

import cats.data.EitherT
import cats.effect.{Concurrent, ContextShift, IO}
import cats.syntax.all._
import cats.{MonadThrow, Show}
import ch.datascience.compression.Zip
import ch.datascience.db.{SessionResource, SqlStatement}
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest, UnsupportedEventType}
import ch.datascience.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess, EventSchedulingResult}
import ch.datascience.events.{EventRequestContent, consumers}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CategoryName, CompoundEventId, EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.circe.DecodingFailure
import io.renku.eventlog.events.categories.statuschange.DBUpdater.EventUpdaterFactory
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent._
import io.renku.eventlog.{EventLogDB, EventMessage}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private class EventHandler[Interpretation[_]: MonadThrow: ContextShift: Concurrent: Logger](
    override val categoryName: CategoryName,
    statusChanger:             StatusChanger[Interpretation],
    deliveryInfoRemover:       DeliveryInfoRemover[Interpretation],
    queriesExecTimes:          LabeledHistogram[Interpretation, SqlStatement.Name]
) extends consumers.EventHandlerWithProcessLimiter[Interpretation](ConcurrentProcessesLimiter.withoutLimit) {

  import EventHandler._

  override def createHandlingProcess(
      request: EventRequestContent
  ): Interpretation[EventHandlingProcess[Interpretation]] = EventHandlingProcess[Interpretation] {
    tryHandle(
      requestAs[ToTriplesGenerated],
      requestAs[ToTriplesStore],
      requestAs[ToFailure[ProcessingStatus, FailureStatus]],
      requestAs[RollbackToNew],
      requestAs[RollbackToTriplesGenerated],
      requestAs[ToAwaitingDeletion],
      requestAs[AllEventsToNew]
    )(request)
  }

  private def tryHandle(
      options: EventRequestContent => EitherT[Interpretation, EventSchedulingResult, Accepted]*
  ): EventRequestContent => EitherT[Interpretation, EventSchedulingResult, Accepted] = request =>
    options.foldLeft(
      EitherT.left[Accepted](UnsupportedEventType.pure[Interpretation].widen[EventSchedulingResult])
    ) { case (previousOptionResult, option) => previousOptionResult orElse option(request) }

  private def requestAs[E <: StatusChangeEvent](request: EventRequestContent)(implicit
      updaterFactory:                                    EventUpdaterFactory[Interpretation, E],
      show:                                              Show[E],
      decoder:                                           EventRequestContent => Either[DecodingFailure, E]
  ): EitherT[Interpretation, EventSchedulingResult, Accepted] = EitherT(
    decode[E](request)
      .map(startUpdate)
      .leftMap(_ => BadRequest)
      .leftWiden[EventSchedulingResult]
      .sequence
  )

  private def startUpdate[E <: StatusChangeEvent](implicit
      updaterFactory: EventUpdaterFactory[Interpretation, E],
      show:           Show[E]
  ): E => Interpretation[Accepted] = event =>
    (ContextShift[Interpretation].shift *> Concurrent[Interpretation]
      .start(executeUpdate(event)))
      .map(_ => Accepted)
      .flatTap(Logger[Interpretation].log(event.show))

  private def executeUpdate[E <: StatusChangeEvent](
      event:                 E
  )(implicit updaterFactory: EventUpdaterFactory[Interpretation, E], show: Show[E]) = statusChanger
    .updateStatuses(event)(updaterFactory(deliveryInfoRemover, queriesExecTimes))
    .recoverWith { case NonFatal(e) => Logger[Interpretation].logError(event, e) >> e.raiseError[Interpretation, Unit] }
    .flatTap(_ => Logger[Interpretation].logInfo(event, "Processed"))
}

private object EventHandler {

  def apply(sessionResource:                    SessionResource[IO, EventLogDB],
            queriesExecTimes:                   LabeledHistogram[IO, SqlStatement.Name],
            awaitingTriplesGenerationGauge:     LabeledGauge[IO, projects.Path],
            underTriplesGenerationGauge:        LabeledGauge[IO, projects.Path],
            awaitingTriplesTransformationGauge: LabeledGauge[IO, projects.Path],
            underTriplesTransformationGauge:    LabeledGauge[IO, projects.Path]
  )(implicit cs:                                ContextShift[IO], logger: Logger[IO]): IO[EventHandler[IO]] = for {
    deliveryInfoRemover <- DeliveryInfoRemover(queriesExecTimes)
    gaugesUpdater <- IO(
                       new GaugesUpdaterImpl[IO](awaitingTriplesGenerationGauge,
                                                 awaitingTriplesTransformationGauge,
                                                 underTriplesTransformationGauge,
                                                 underTriplesGenerationGauge
                       )
                     )
    statusChanger <- IO(new StatusChangerImpl[IO](sessionResource, gaugesUpdater))
  } yield new EventHandler[IO](categoryName, statusChanger, deliveryInfoRemover, queriesExecTimes)

  import ch.datascience.tinytypes.json.TinyTypeDecoders._

  private def decode[E <: StatusChangeEvent](request: EventRequestContent)(implicit
      decoder:                                        EventRequestContent => Either[DecodingFailure, E]
  ) = decoder(request)

  private implicit lazy val eventTriplesGeneratedDecoder
      : EventRequestContent => Either[DecodingFailure, ToTriplesGenerated] = {
    case EventRequestContent.WithPayload(event, payload) =>
      for {
        id             <- event.hcursor.downField("id").as[EventId]
        projectId      <- event.hcursor.downField("project").downField("id").as[projects.Id]
        projectPath    <- event.hcursor.downField("project").downField("path").as[projects.Path]
        processingTime <- event.hcursor.downField("processingTime").as[EventProcessingTime]
        _ <- event.hcursor.downField("newStatus").as[EventStatus].flatMap {
               case TriplesGenerated => Right(())
               case status           => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
        eventPayload <- Zip.unzip(payload)
      } yield ToTriplesGenerated(CompoundEventId(id, projectId), projectPath, processingTime, eventPayload)
    case _ => Left(DecodingFailure("Missing event payload", Nil))
  }

  private implicit lazy val eventTripleStoreDecoder: EventRequestContent => Either[DecodingFailure, ToTriplesStore] = {
    case EventRequestContent(event, _) =>
      for {
        id             <- event.hcursor.downField("id").as[EventId]
        projectId      <- event.hcursor.downField("project").downField("id").as[projects.Id]
        projectPath    <- event.hcursor.downField("project").downField("path").as[projects.Path]
        processingTime <- event.hcursor.downField("processingTime").as[EventProcessingTime]
        _ <- event.hcursor.downField("newStatus").as[EventStatus].flatMap {
               case TriplesStore => Right(())
               case status       => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield ToTriplesStore(CompoundEventId(id, projectId), projectPath, processingTime)
  }

  private implicit lazy val eventFailureDecoder
      : EventRequestContent => Either[DecodingFailure, ToFailure[ProcessingStatus, FailureStatus]] = {
    case EventRequestContent(event, _) =>
      for {
        id          <- event.hcursor.downField("id").as[EventId]
        projectId   <- event.hcursor.downField("project").downField("id").as[projects.Id]
        projectPath <- event.hcursor.downField("project").downField("path").as[projects.Path]
        message     <- event.hcursor.downField("message").as[EventMessage]
        eventId = CompoundEventId(id, projectId)
        statusChangeEvent <- event.hcursor.downField("newStatus").as[EventStatus].flatMap {
                               case status: GenerationRecoverableFailure =>
                                 ToFailure(eventId, projectPath, message, GeneratingTriples, status).asRight
                               case status: GenerationNonRecoverableFailure =>
                                 ToFailure(eventId, projectPath, message, GeneratingTriples, status).asRight
                               case status: TransformationRecoverableFailure =>
                                 ToFailure(eventId, projectPath, message, TransformingTriples, status).asRight
                               case status: TransformationNonRecoverableFailure =>
                                 ToFailure(eventId, projectPath, message, TransformingTriples, status).asRight
                               case status =>
                                 DecodingFailure(s"Unrecognized event status $status", Nil)
                                   .asLeft[ToFailure[ProcessingStatus, FailureStatus]]
                             }
      } yield statusChangeEvent
  }

  private implicit lazy val eventRollbackToNewDecoder: EventRequestContent => Either[DecodingFailure, RollbackToNew] = {
    case EventRequestContent(event, _) =>
      for {
        id          <- event.hcursor.downField("id").as[EventId]
        projectId   <- event.hcursor.downField("project").downField("id").as[projects.Id]
        projectPath <- event.hcursor.downField("project").downField("path").as[projects.Path]
        _ <- event.hcursor.downField("newStatus").as[EventStatus].flatMap {
               case New    => Right(())
               case status => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield RollbackToNew(CompoundEventId(id, projectId), projectPath)
  }

  private implicit lazy val eventRollbackToTriplesGeneratedDecoder
      : EventRequestContent => Either[DecodingFailure, RollbackToTriplesGenerated] = {
    case EventRequestContent(event, _) =>
      for {
        id          <- event.hcursor.downField("id").as[EventId]
        projectId   <- event.hcursor.downField("project").downField("id").as[projects.Id]
        projectPath <- event.hcursor.downField("project").downField("path").as[projects.Path]
        _ <- event.hcursor.downField("newStatus").as[EventStatus].flatMap {
               case TriplesGenerated => Right(())
               case status           => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield RollbackToTriplesGenerated(CompoundEventId(id, projectId), projectPath)
  }

  private implicit lazy val eventToAwaitingDeletionDecoder
      : EventRequestContent => Either[DecodingFailure, ToAwaitingDeletion] = { case EventRequestContent(event, _) =>
    for {
      id          <- event.hcursor.downField("id").as[EventId]
      projectId   <- event.hcursor.downField("project").downField("id").as[projects.Id]
      projectPath <- event.hcursor.downField("project").downField("path").as[projects.Path]
      _ <- event.hcursor.downField("newStatus").as[EventStatus].flatMap {
             case AwaitingDeletion => Right(())
             case status           => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
           }
    } yield ToAwaitingDeletion(CompoundEventId(id, projectId), projectPath)
  }

  private implicit lazy val allEventNewDecoder: EventRequestContent => Either[DecodingFailure, AllEventsToNew] =
    _.event.hcursor.downField("newStatus").as[EventStatus] >>= {
      case New    => Right(AllEventsToNew)
      case status => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
    }
}
