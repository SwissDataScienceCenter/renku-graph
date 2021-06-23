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
import cats.data.EitherT.fromEither
import cats.effect.{Concurrent, ContextShift, IO}
import cats.syntax.all._
import cats.{MonadThrow, Show}
import ch.datascience.db.SqlStatement.Name
import ch.datascience.db.{SessionResource, SqlStatement}
import ch.datascience.events.consumers
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest, UnsupportedEventType}
import ch.datascience.events.consumers.{EventRequestContent, EventSchedulingResult}
import ch.datascience.graph.model.events.{CategoryName, CompoundEventId, EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.{SchemaVersion, projects}
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.circe.{DecodingFailure, parser}
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent._
import io.renku.eventlog.{EventLogDB, EventPayload}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private class EventHandler[Interpretation[_]: MonadThrow: ContextShift: Concurrent](
    override val categoryName: CategoryName,
    statusChanger:             StatusChanger[Interpretation],
    queriesExecTimes:          LabeledHistogram[Interpretation, SqlStatement.Name],
    logger:                    Logger[Interpretation]
) extends consumers.EventHandler[Interpretation] {

  import EventHandler._

  private implicit lazy val execTimes: LabeledHistogram[Interpretation, SqlStatement.Name] = queriesExecTimes

  override def handle(request: EventRequestContent): Interpretation[EventSchedulingResult] = {
    fromEither[Interpretation](request.event.validateCategoryName) >> tryHandle(
      requestAs[ToTriplesGenerated],
      requestAs[ToTriplesStore],
      requestAs[ToNew],
      requestAs[ToAwaitingDeletion],
      requestAs[AllEventsToNew]
    )(request)
  }.merge

  private def tryHandle(
      options: EventRequestContent => EitherT[Interpretation, EventSchedulingResult, EventSchedulingResult]*
  ): EventRequestContent => EitherT[Interpretation, EventSchedulingResult, EventSchedulingResult] = request =>
    options.foldLeft(
      EitherT.left[EventSchedulingResult](UnsupportedEventType.pure[Interpretation].widen[EventSchedulingResult])
    ) { case (acc, option) => acc orElse option(request) }

  private def requestAs[E <: StatusChangeEvent](request: EventRequestContent)(implicit
      updaterFactory:                                    LabeledHistogram[Interpretation, SqlStatement.Name] => DBUpdater[Interpretation, E],
      show:                                              Show[E],
      decoder:                                           EventRequestContent => Either[DecodingFailure, E]
  ): EitherT[Interpretation, EventSchedulingResult, EventSchedulingResult] = EitherT(
    decode[E](request)
      .map(startUpdate)
      .leftMap(_ => BadRequest)
      .leftWiden[EventSchedulingResult]
      .sequence
  )

  private def startUpdate[E <: StatusChangeEvent](implicit
      updaterFactory: LabeledHistogram[Interpretation, Name] => DBUpdater[Interpretation, E],
      show:           Show[E]
  ): E => Interpretation[EventSchedulingResult] = event =>
    (ContextShift[Interpretation].shift *> Concurrent[Interpretation]
      .start(executeUpdate(event)))
      .map(_ => Accepted)
      .widen[EventSchedulingResult]
      .flatTap(logger.log(event.show))

  private def executeUpdate[E <: StatusChangeEvent](
      event:                 E
  )(implicit updaterFactory: LabeledHistogram[Interpretation, Name] => DBUpdater[Interpretation, E], show: Show[E]) =
    statusChanger
      .updateStatuses(event)(updaterFactory(execTimes))
      .recoverWith { case NonFatal(e) => logger.logError(event, e) >> e.raiseError[Interpretation, Unit] }
      .flatTap(_ => logger.logInfo(event, "Processed"))
}

private object EventHandler {

  def apply(sessionResource:                    SessionResource[IO, EventLogDB],
            queriesExecTimes:                   LabeledHistogram[IO, SqlStatement.Name],
            awaitingTriplesGenerationGauge:     LabeledGauge[IO, projects.Path],
            underTriplesGenerationGauge:        LabeledGauge[IO, projects.Path],
            awaitingTriplesTransformationGauge: LabeledGauge[IO, projects.Path],
            underTriplesTransformationGauge:    LabeledGauge[IO, projects.Path],
            logger:                             Logger[IO]
  )(implicit cs:                                ContextShift[IO]): IO[EventHandler[IO]] = for {
    gaugesUpdater <- IO(
                       new GaugesUpdaterImpl[IO](awaitingTriplesGenerationGauge,
                                                 awaitingTriplesTransformationGauge,
                                                 underTriplesTransformationGauge,
                                                 underTriplesGenerationGauge
                       )
                     )
    statusChanger <- IO(new StatusChangerImpl[IO](sessionResource, gaugesUpdater))
  } yield new EventHandler[IO](categoryName, statusChanger, queriesExecTimes, logger)

  import ch.datascience.tinytypes.json.TinyTypeDecoders._

  private def decode[E <: StatusChangeEvent](request: EventRequestContent)(implicit
      decoder:                                        EventRequestContent => Either[DecodingFailure, E]
  ) =
    decoder(request)

  private implicit lazy val eventTriplesGeneratedDecoder
      : EventRequestContent => Either[DecodingFailure, ToTriplesGenerated] = {
    case EventRequestContent(event, Some(payload)) =>
      for {
        id             <- event.hcursor.downField("id").as[EventId]
        projectId      <- event.hcursor.downField("project").downField("id").as[projects.Id]
        projectPath    <- event.hcursor.downField("project").downField("path").as[projects.Path]
        processingTime <- event.hcursor.downField("processingTime").as[EventProcessingTime]
        _ <- event.hcursor.downField("newStatus").as[EventStatus].flatMap {
               case EventStatus.TriplesGenerated => Right(())
               case status                       => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
        payloadJson          <- parser.parse(payload).leftMap(p => DecodingFailure(s"Could not parse payload: $p", Nil))
        eventPayload         <- payloadJson.hcursor.downField("payload").as[EventPayload]
        payloadSchemaVersion <- payloadJson.hcursor.downField("schemaVersion").as[SchemaVersion]
      } yield ToTriplesGenerated(CompoundEventId(id, projectId),
                                 projectPath,
                                 processingTime,
                                 eventPayload,
                                 payloadSchemaVersion
      )
    case _ => Left(DecodingFailure(s"Missing event payload", Nil))
  }

  private implicit lazy val eventTripleStoreDecoder: EventRequestContent => Either[DecodingFailure, ToTriplesStore] = {
    case EventRequestContent(event, _) =>
      for {
        id             <- event.hcursor.downField("id").as[EventId]
        projectId      <- event.hcursor.downField("project").downField("id").as[projects.Id]
        projectPath    <- event.hcursor.downField("project").downField("path").as[projects.Path]
        processingTime <- event.hcursor.downField("processingTime").as[EventProcessingTime]
        _ <- event.hcursor.downField("newStatus").as[EventStatus].flatMap {
               case EventStatus.TriplesStore => Right(())
               case status                   => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield ToTriplesStore(CompoundEventId(id, projectId), projectPath, processingTime)
  }

  private implicit lazy val eventToNewDecoder: EventRequestContent => Either[DecodingFailure, ToNew] = {
    case EventRequestContent(event, _) =>
      for {
        id          <- event.hcursor.downField("id").as[EventId]
        projectId   <- event.hcursor.downField("project").downField("id").as[projects.Id]
        projectPath <- event.hcursor.downField("project").downField("path").as[projects.Path]
        _ <- event.hcursor.downField("newStatus").as[EventStatus].flatMap {
               case EventStatus.New => Right(())
               case status          => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
             }
      } yield ToNew(CompoundEventId(id, projectId), projectPath)
  }

  private implicit lazy val eventToAwaitingDeletionDecoder
      : EventRequestContent => Either[DecodingFailure, ToAwaitingDeletion] = { case EventRequestContent(event, _) =>
    for {
      id          <- event.hcursor.downField("id").as[EventId]
      projectId   <- event.hcursor.downField("project").downField("id").as[projects.Id]
      projectPath <- event.hcursor.downField("project").downField("path").as[projects.Path]
      _ <- event.hcursor.downField("newStatus").as[EventStatus].flatMap {
             case EventStatus.AwaitingDeletion => Right(())
             case status                       => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
           }
    } yield ToAwaitingDeletion(CompoundEventId(id, projectId), projectPath)
  }

  private implicit lazy val allEventNewDecoder: EventRequestContent => Either[DecodingFailure, AllEventsToNew] =
    _.event.hcursor.downField("newStatus").as[EventStatus] >>= {
      case EventStatus.New => Right(AllEventsToNew)
      case status          => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
    }
}
