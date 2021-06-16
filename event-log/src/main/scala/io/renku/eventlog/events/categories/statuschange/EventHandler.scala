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
import ch.datascience.graph.model.events.{CategoryName, CompoundEventId, EventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.circe.{Decoder, DecodingFailure}
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent._
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
      requestAs[AncestorsToTriplesGenerated],
      requestAs[AncestorsToTriplesStore]
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
      decoder:                                           Decoder[E]
  ): EitherT[Interpretation, EventSchedulingResult, EventSchedulingResult] = EitherT(
    request.event
      .as[E]
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
      .recoverWith { case NonFatal(e) =>
        logger.logError(event, e) >> e.raiseError[Interpretation, Unit]
      } >> logger.logInfo(event, "Processed")
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

  private implicit val eventTriplesGeneratedDecoder: Decoder[AncestorsToTriplesGenerated] = { cursor =>
    for {
      id          <- cursor.downField("id").as[EventId]
      projectId   <- cursor.downField("project").downField("id").as[projects.Id]
      projectPath <- cursor.downField("project").downField("path").as[projects.Path]
      _ <- cursor.downField("newStatus").as[EventStatus].flatMap {
             case EventStatus.TriplesGenerated => Right(())
             case status                       => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
           }
    } yield AncestorsToTriplesGenerated(CompoundEventId(id, projectId), projectPath)
  }

  private implicit val eventTripleStoreDecoder: Decoder[AncestorsToTriplesStore] = { cursor =>
    for {
      id          <- cursor.downField("id").as[EventId]
      projectId   <- cursor.downField("project").downField("id").as[projects.Id]
      projectPath <- cursor.downField("project").downField("path").as[projects.Path]
      _ <- cursor.downField("newStatus").as[EventStatus].flatMap {
             case EventStatus.TriplesStore => Right(())
             case status                   => Left(DecodingFailure(s"Unrecognized event status $status", Nil))
           }
    } yield AncestorsToTriplesStore(CompoundEventId(id, projectId), projectPath)
  }
}
