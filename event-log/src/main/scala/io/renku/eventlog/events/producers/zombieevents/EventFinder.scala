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

package io.renku.eventlog.events.producers
package zombieevents

import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.events.producers
import io.renku.eventlog.metrics.QueriesExecutionTimes
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private class EventFinder[F[_]: MonadThrow: Logger](
    longProcessingEventsFinder: producers.EventFinder[F, ZombieEvent],
    lostSubscriberEventFinder:  producers.EventFinder[F, ZombieEvent],
    zombieNodesCleaner:         ZombieNodesCleaner[F],
    lostZombieEventFinder:      producers.EventFinder[F, ZombieEvent]
) extends producers.EventFinder[F, ZombieEvent] {

  import zombieNodesCleaner._

  override def popEvent(): F[Option[ZombieEvent]] = for {
    _ <- removeZombieNodes() recoverWith logError
    maybeEvent <- OptionT(longProcessingEventsFinder.popEvent())
                    .orElseF(lostSubscriberEventFinder.popEvent())
                    .orElseF(lostZombieEventFinder.popEvent())
                    .value
  } yield maybeEvent

  private lazy val logError: PartialFunction[Throwable, F[Unit]] = { case NonFatal(e) =>
    Logger[F].error(e)("ZombieEventSourceCleaner - failure during clean up")
  }
}

private object EventFinder {

  def apply[F[_]: Async: Parallel: SessionResource: Logger: QueriesExecutionTimes]
      : F[producers.EventFinder[F, ZombieEvent]] = for {
    longProcessingEventFinder <- LongProcessingEventFinder[F]
    lostSubscriberEventFinder <- LostSubscriberEventFinder[F]
    zombieNodesCleaner        <- ZombieNodesCleaner[F]
    lostZombieEventFinder     <- LostZombieEventFinder[F]
  } yield new EventFinder[F](longProcessingEventFinder,
                             lostSubscriberEventFinder,
                             zombieNodesCleaner,
                             lostZombieEventFinder
  )
}

private trait ZombieEventSubProcess {
  val processName: ZombieEventProcess
}
