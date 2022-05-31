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

package io.renku.eventlog.events.categories.zombieevents

import cats.Show
import cats.data.EitherT.fromEither
import cats.effect.Async
import cats.effect.kernel.Spawn
import cats.syntax.all._
import io.circe.Decoder
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess, EventSchedulingResult}
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{CompoundEventId, EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private class EventHandler[F[_]: Async: Logger](
    override val categoryName:   CategoryName,
    zombieStatusCleaner:         ZombieStatusCleaner[F],
    awaitingGenerationGauge:     LabeledGauge[F, projects.Path],
    underGenerationGauge:        LabeledGauge[F, projects.Path],
    awaitingTransformationGauge: LabeledGauge[F, projects.Path],
    underTransformationGauge:    LabeledGauge[F, projects.Path],
    awaitingDeletionGauge:       LabeledGauge[F, projects.Path],
    underDeletionGauge:          LabeledGauge[F, projects.Path]
) extends consumers.EventHandlerWithProcessLimiter[F](ConcurrentProcessesLimiter.withoutLimit) {

  override def createHandlingProcess(request: EventRequestContent): F[EventHandlingProcess[F]] =
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
    zombieStatusCleaner.cleanZombieStatus(event) >>= {
      case Updated => updateGauges(event)
      case _       => ().pure[F]
    }
  } recoverWith { case NonFatal(exception) => Logger[F].logError(event, exception) }

  private lazy val updateGauges: ZombieEvent => F[Unit] = {
    case ZombieEvent(_, projectPath, GeneratingTriples) =>
      awaitingGenerationGauge.increment(projectPath) >> underGenerationGauge.decrement(projectPath)
    case ZombieEvent(_, projectPath, TransformingTriples) =>
      awaitingTransformationGauge.increment(projectPath) >> underTransformationGauge.decrement(projectPath)
    case ZombieEvent(_, projectPath, Deleting) =>
      awaitingDeletionGauge.increment(projectPath) >> underDeletionGauge.decrement(projectPath)
  }

  private implicit lazy val eventInfoToString: Show[ZombieEvent] = Show.show { event =>
    show"${event.eventId}, projectPath = ${event.projectPath}, status = ${event.status}"
  }

  private implicit lazy val eventDecoder: Decoder[ZombieEvent] = { cursor =>
    import io.renku.tinytypes.json.TinyTypeDecoders._

    implicit val processingStatusDecoder: Decoder[EventStatus.ProcessingStatus] = Decoder[EventStatus].emap {
      case s: ProcessingStatus => s.asRight
      case s => show"'$s' is not a ProcessingStatus".asLeft
    }

    for {
      id          <- cursor.downField("id").as[EventId]
      projectId   <- cursor.downField("project").downField("id").as[projects.Id]
      projectPath <- cursor.downField("project").downField("path").as[projects.Path]
      status      <- cursor.downField("status").as[EventStatus.ProcessingStatus]
    } yield ZombieEvent(CompoundEventId(id, projectId), projectPath, status)
  }
}

private object EventHandler {
  def apply[F[_]: Async: SessionResource: Logger](
      queriesExecTimes:            LabeledHistogram[F],
      awaitingGenerationGauge:     LabeledGauge[F, projects.Path],
      underGenerationGauge:        LabeledGauge[F, projects.Path],
      awaitingTransformationGauge: LabeledGauge[F, projects.Path],
      underTransformationGauge:    LabeledGauge[F, projects.Path],
      awaitingDeletionGauge:       LabeledGauge[F, projects.Path],
      underDeletionGauge:          LabeledGauge[F, projects.Path]
  ): F[EventHandler[F]] = for {
    zombieStatusCleaner <- ZombieStatusCleaner(queriesExecTimes)
  } yield new EventHandler[F](categoryName,
                              zombieStatusCleaner,
                              awaitingGenerationGauge,
                              underGenerationGauge,
                              awaitingTransformationGauge,
                              underTransformationGauge,
                              awaitingDeletionGauge,
                              underDeletionGauge
  )
}
