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

package io.renku.entities.viewings.collector.projects.viewed

import cats.effect.{Async, Temporal}
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.{CategoryName, consumers}
import io.renku.eventsqueue.EventsQueue
import io.renku.lock.Lock
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

object SubscriptionFactory {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder, DB](
      tsReadyCheck:    F[Boolean],
      categoryLock:    Lock[F, CategoryName],
      sessionResource: SessionResource[F, DB],
      connConfig:      ProjectsConnectionConfig
  ): F[(consumers.EventHandler[F], SubscriptionMechanism[F])] = {
    implicit val sr: SessionResource[F, DB] = sessionResource
    for {
      eventsQueue    <- EventsQueue[F, DB](categoryLock).pure[F]
      handler        <- EventHandler[F](eventsQueue)
      eventProcessor <- EventProcessor[F](connConfig, eventsQueue)
      _              <- kickOffEventsDequeueing[F](tsReadyCheck, eventsQueue, eventProcessor)
    } yield handler -> SubscriptionMechanism.noOpSubscriptionMechanism(categoryName)
  }

  private[viewed] def kickOffEventsDequeueing[F[_]: Async: Logger](tsReadyCheck:     F[Boolean],
                                                                   eventsQueue:      EventsQueue[F],
                                                                   processor:        EventProcessor[F],
                                                                   onErrorWait:      Duration = 2 seconds,
                                                                   onTSNotReadyWait: Duration = 10 seconds
  ) = Async[F].start {
    startDequeueingEvents(tsReadyCheck, eventsQueue, processor, onErrorWait, onTSNotReadyWait)
  }.void

  private def startDequeueingEvents[F[_]: Async: Logger](tsReadyCheck:     F[Boolean],
                                                         eventsQueue:      EventsQueue[F],
                                                         processor:        EventProcessor[F],
                                                         onErrorWait:      Duration,
                                                         onTSNotReadyWait: Duration
  ): F[Unit] = tsReadyCheck
    .flatMap {
      case true =>
        Logger[F].info(show"Starting events dequeueing for $categoryName") >>
          hookToTheEventsStream(eventsQueue, processor)
      case false =>
        Logger[F].info(show"TS not ready for writing; dequeueing for $categoryName on hold") >>
          Temporal[F].delayBy(
            startDequeueingEvents(tsReadyCheck, eventsQueue, processor, onErrorWait, onTSNotReadyWait),
            onTSNotReadyWait
          )
    }
    .handleErrorWith(waitAndRetry(tsReadyCheck, eventsQueue, processor, onErrorWait, onTSNotReadyWait))

  private def waitAndRetry[F[_]: Async: Logger](tsReadyCheck:     F[Boolean],
                                                eventsQueue:      EventsQueue[F],
                                                processor:        EventProcessor[F],
                                                onErrorWait:      Duration,
                                                onTSNotReadyWait: Duration
  ): Throwable => F[Unit] =
    Logger[F].error(_)(show"An error in the $categoryName processing pipe; restarting") >>
      Temporal[F].delayBy(
        startDequeueingEvents(tsReadyCheck, eventsQueue, processor, onErrorWait, onTSNotReadyWait),
        onErrorWait
      )

  private def hookToTheEventsStream[F[_]: Async](eventsQueue: EventsQueue[F], processor: EventProcessor[F]) =
    eventsQueue.acquireEventsStream(categoryName).through(processor).compile.drain
}
