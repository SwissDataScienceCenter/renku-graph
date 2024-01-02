/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import io.renku.eventsqueue.EventsQueue
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

private object EventsDequeueingInitiator {
  def apply[F[_]: Async: Logger](tsReadyCheck:     F[Boolean],
                                 eventsQueue:      EventsQueue[F],
                                 processor:        EventProcessor[F],
                                 onErrorWait:      Duration = 2 seconds,
                                 onTSNotReadyWait: Duration = 10 seconds
  ): EventsDequeueingInitiator[F] =
    new EventsDequeueingInitiator[F](tsReadyCheck, eventsQueue, processor, onErrorWait, onTSNotReadyWait)
}

private class EventsDequeueingInitiator[F[_]: Async: Logger](tsReadyCheck: F[Boolean],
                                                             eventsQueue:      EventsQueue[F],
                                                             processor:        EventProcessor[F],
                                                             onErrorWait:      Duration = 2 seconds,
                                                             onTSNotReadyWait: Duration = 10 seconds
) {

  def initDequeueing: F[Unit] =
    Async[F].start(initIfTSReady).void

  private def initIfTSReady: F[Unit] =
    tsReadyCheck
      .flatMap {
        case true =>
          Logger[F].info(show"Starting events dequeueing for $categoryName") >>
            hookToTheEventsStream
        case false =>
          Logger[F].info(show"TS not ready for writing; dequeueing for $categoryName on hold") >>
            Temporal[F].delayBy(initIfTSReady, onTSNotReadyWait)
      }
      .handleErrorWith(waitAndRetry)

  private lazy val waitAndRetry: Throwable => F[Unit] =
    Logger[F].error(_)(show"An error in the $categoryName processing pipe; restarting") >>
      Temporal[F].delayBy(initIfTSReady, onErrorWait)

  private def hookToTheEventsStream =
    eventsQueue.acquireEventsStream(categoryName).through(processor).compile.drain
}
