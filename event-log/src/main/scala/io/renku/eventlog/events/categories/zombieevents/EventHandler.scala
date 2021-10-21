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
import cats.effect.Concurrent
import cats.effect.kernel.Spawn
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

import scala.util.control.NonFatal

private class EventHandler[F[_]: MonadThrow: Spawn: Concurrent: Logger](
    override val categoryName:          CategoryName,
    zombieStatusCleaner:                ZombieStatusCleaner[F],
    awaitingTriplesGenerationGauge:     LabeledGauge[F, projects.Path],
    underTriplesGenerationGauge:        LabeledGauge[F, projects.Path],
    awaitingTriplesTransformationGauge: LabeledGauge[F, projects.Path],
    underTriplesTransformationGauge:    LabeledGauge[F, projects.Path]
) extends consumers.EventHandlerWithProcessLimiter[F](ConcurrentProcessesLimiter.withoutLimit) {

  import io.renku.graph.model.projects

  override def createHandlingProcess(
      request: EventRequestContent
  ): F[EventHandlingProcess[F]] =
    EventHandlingProcess[F](startCleanZombieEvents(request))

  private def startCleanZombieEvents(request: EventRequestContent) = for {
    event <- fromEither[F](
               request.event.as[ZombieEvent].leftMap(_ => BadRequest).leftWiden[EventSchedulingResult]
             )
    result <- Spawn[F]
                .start(cleanZombieStatus(event))
                .toRightT
                .map(_ => Accepted)
                .semiflatTap(Logger[F].log(event))
                .leftSemiflatTap(Logger[F].log(event))
  } yield result

  private def cleanZombieStatus(event: ZombieEvent): F[Unit] = {
    for {
      result <- zombieStatusCleaner.cleanZombieStatus(event)
      _      <- Applicative[F].whenA(result == Updated)(updateGauges(event))
      _      <- Logger[F].logInfo(event, result.toString)
    } yield ()
  } recoverWith { case NonFatal(exception) => Logger[F].logError(event, exception) }

  private lazy val updateGauges: ZombieEvent => F[Unit] = {
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

  private implicit lazy val eventDecoder: Decoder[ZombieEvent] = { cursor =>
    import io.renku.tinytypes.json.TinyTypeDecoders._

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
  def apply[F[_]: Spawn: Concurrent: Logger](
      sessionResource:                    SessionResource[F, EventLogDB],
      queriesExecTimes:                   LabeledHistogram[F, SqlStatement.Name],
      awaitingTriplesGenerationGauge:     LabeledGauge[F, projects.Path],
      underTriplesGenerationGauge:        LabeledGauge[F, projects.Path],
      awaitingTriplesTransformationGauge: LabeledGauge[F, projects.Path],
      underTriplesTransformationGauge:    LabeledGauge[F, projects.Path]
  ): F[EventHandler[F]] = for {
    zombieStatusCleaner <- ZombieStatusCleaner(sessionResource, queriesExecTimes)
  } yield new EventHandler[F](categoryName,
                              zombieStatusCleaner,
                              awaitingTriplesGenerationGauge,
                              underTriplesGenerationGauge,
                              awaitingTriplesTransformationGauge,
                              underTriplesTransformationGauge
  )
}
