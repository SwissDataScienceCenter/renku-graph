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

package io.renku.eventlog.events.categories.zombieevents

import cats.data.EitherT.fromEither
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import cats.syntax.all._
import cats.{Applicative, MonadThrow, Show}
import io.circe.{Decoder, DecodingFailure}
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.eventlog._
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess, EventSchedulingResult}
import io.renku.events.{EventRequestContent, consumers}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{CategoryName, CompoundEventId, EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private class EventHandler[Interpretation[_]: MonadThrow: ContextShift: Concurrent](
    override val categoryName:          CategoryName,
    zombieStatusCleaner:                ZombieStatusCleaner[Interpretation],
    awaitingTriplesGenerationGauge:     LabeledGauge[Interpretation, projects.Path],
    underTriplesGenerationGauge:        LabeledGauge[Interpretation, projects.Path],
    awaitingTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    underTriplesTransformationGauge:    LabeledGauge[Interpretation, projects.Path],
    logger:                             Logger[Interpretation]
) extends consumers.EventHandlerWithProcessLimiter[Interpretation](ConcurrentProcessesLimiter.withoutLimit) {

  import io.renku.graph.model.projects
  import io.renku.tinytypes.json.TinyTypeDecoders._

  override def createHandlingProcess(
      request: EventRequestContent
  ): Interpretation[EventHandlingProcess[Interpretation]] =
    EventHandlingProcess[Interpretation](startCleanZombieEvents(request))

  private def startCleanZombieEvents(request: EventRequestContent) = for {
    event <- fromEither[Interpretation](
               request.event.as[ZombieEvent].leftMap(_ => BadRequest).leftWiden[EventSchedulingResult]
             )
    result <- (ContextShift[Interpretation].shift *> Concurrent[Interpretation]
                .start(cleanZombieStatus(event))).toRightT
                .map(_ => Accepted)
                .semiflatTap(logger.log(event))
                .leftSemiflatTap(logger.log(event))
  } yield result

  private def cleanZombieStatus(event: ZombieEvent): Interpretation[Unit] = {
    for {
      result <- zombieStatusCleaner.cleanZombieStatus(event)
      _      <- Applicative[Interpretation].whenA(result == Updated)(updateGauges(event))
      _      <- logger.logInfo(event, result.toString)
    } yield ()
  } recoverWith { case NonFatal(exception) => logger.logError(event, exception) }

  private lazy val updateGauges: ZombieEvent => Interpretation[Unit] = {
    case GeneratingTriplesZombieEvent(_, projectPath) =>
      for {
        _ <- awaitingTriplesGenerationGauge.increment(projectPath)
        _ <- underTriplesGenerationGauge.decrement(projectPath)
      } yield ()
    case TransformingTriplesZombieEvent(_, projectPath) =>
      for {
        _ <- awaitingTriplesTransformationGauge.increment(projectPath)
        _ <- underTriplesTransformationGauge.decrement(projectPath)
      } yield ()
  }

  private implicit lazy val eventInfoToString: Show[ZombieEvent] = Show.show { event =>
    show"${event.eventId}, projectPath = ${event.projectPath}, status = ${event.status}"
  }

  private implicit val eventDecoder: Decoder[ZombieEvent] = { cursor =>
    for {
      id          <- cursor.downField("id").as[EventId]
      projectId   <- cursor.downField("project").downField("id").as[projects.Id]
      projectPath <- cursor.downField("project").downField("path").as[projects.Path]
      status      <- cursor.downField("status").as[EventStatus]
      zombieEvent <- status match {
                       case GeneratingTriples =>
                         GeneratingTriplesZombieEvent(CompoundEventId(id, projectId), projectPath)
                           .asRight[DecodingFailure]
                       case TransformingTriples =>
                         TransformingTriplesZombieEvent(CompoundEventId(id, projectId), projectPath)
                           .asRight[DecodingFailure]
                       case _ =>
                         DecodingFailure(s"$status is an illegal status for ZombieEvent", Nil).asLeft[ZombieEvent]
                     }
    } yield zombieEvent
  }
}

private object EventHandler {
  def apply(sessionResource:                    SessionResource[IO, EventLogDB],
            queriesExecTimes:                   LabeledHistogram[IO, SqlStatement.Name],
            awaitingTriplesGenerationGauge:     LabeledGauge[IO, projects.Path],
            underTriplesGenerationGauge:        LabeledGauge[IO, projects.Path],
            awaitingTriplesTransformationGauge: LabeledGauge[IO, projects.Path],
            underTriplesTransformationGauge:    LabeledGauge[IO, projects.Path],
            logger:                             Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventHandler[IO]] = for {
    zombieStatusCleaner <- ZombieStatusCleaner(sessionResource, queriesExecTimes)
  } yield new EventHandler[IO](categoryName,
                               zombieStatusCleaner,
                               awaitingTriplesGenerationGauge,
                               underTriplesGenerationGauge,
                               awaitingTriplesTransformationGauge,
                               underTriplesTransformationGauge,
                               logger
  )
}
