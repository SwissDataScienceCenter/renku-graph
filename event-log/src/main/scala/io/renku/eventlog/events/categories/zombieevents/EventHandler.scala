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

import cats.MonadError
import cats.data.EitherT.fromEither
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.events.consumers
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import ch.datascience.events.consumers.{EventRequestContent, EventSchedulingResult}
import ch.datascience.graph.model.events.{CategoryName, CompoundEventId, EventId, EventStatus}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.chrisdavenport.log4cats.Logger
import io.circe.{Decoder, DecodingFailure}
import io.renku.eventlog._

import scala.concurrent.ExecutionContext

private class EventHandler[Interpretation[_]](
    override val categoryName: CategoryName,
    statusUpdater:             EventStatusUpdater[Interpretation],
    logger:                    Logger[Interpretation]
)(implicit
    ME:           MonadError[Interpretation, Throwable],
    contextShift: ContextShift[Interpretation],
    concurrent:   Concurrent[Interpretation]
) extends consumers.EventHandler[Interpretation] {

  import ch.datascience.graph.model.projects
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import statusUpdater._

  override def handle(request: EventRequestContent): Interpretation[EventSchedulingResult] = {
    for {
      _ <- fromEither[Interpretation](request.event.validateCategoryName)
      event <- fromEither[Interpretation](
                 request.event.as[ZombieEvent].leftMap(_ => BadRequest).leftWiden[EventSchedulingResult]
               )
      result <- (contextShift.shift *> concurrent
                  .start(changeStatus(event))).toRightT
                  .map(_ => Accepted)
                  .semiflatTap(logger.log(event))
                  .leftSemiflatTap(logger.log(event))
    } yield result
  }.merge

  private implicit lazy val eventInfoToString: ZombieEvent => String = { event =>
    s"${event.eventId}, status = ${event.status}"
  }

  private implicit val eventDecoder: Decoder[ZombieEvent] = { cursor =>
    for {
      id        <- cursor.downField("id").as[EventId]
      projectId <- cursor.downField("project").downField("id").as[projects.Id]
      status    <- cursor.downField("status").as[EventStatus]
      _ <- if (status == GeneratingTriples || status == TransformingTriples) ().asRight[DecodingFailure]
           else DecodingFailure(s"$status is an illegal status for ZombieEvent", Nil).asLeft[Unit]
    } yield ZombieEvent(CompoundEventId(id, projectId), status)
  }
}

private object EventHandler {
  def apply(transactor:         DbTransactor[IO, EventLogDB],
            waitingEventsGauge: LabeledGauge[IO, projects.Path],
            queriesExecTimes:   LabeledHistogram[IO, SqlQuery.Name],
            logger:             Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventHandler[IO]] = for {
    statusUpdater <- EventStatusUpdater(transactor, waitingEventsGauge, queriesExecTimes)
  } yield new EventHandler[IO](categoryName, statusUpdater, logger)
}
