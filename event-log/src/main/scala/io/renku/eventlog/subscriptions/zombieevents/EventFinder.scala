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

package io.renku.eventlog.subscriptions.zombieevents

import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.subscriptions
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private class EventFinder[F[_]: MonadThrow: Logger](
    longProcessingEventsFinder: subscriptions.EventFinder[F, ZombieEvent],
    lostSubscriberEventFinder:  subscriptions.EventFinder[F, ZombieEvent],
    zombieNodesCleaner:         ZombieNodesCleaner[F],
    lostZombieEventFinder:      subscriptions.EventFinder[F, ZombieEvent]
) extends subscriptions.EventFinder[F, ZombieEvent] {

  override def popEvent(): F[Option[ZombieEvent]] = for {
    _ <- zombieNodesCleaner.removeZombieNodes() recoverWith logError
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

  def apply[F[_]: Async: Parallel: SessionResource: Logger](
      queriesExecTimes: LabeledHistogram[F]
  ): F[subscriptions.EventFinder[F, ZombieEvent]] = for {
    longProcessingEventFinder <- LongProcessingEventFinder(queriesExecTimes)
    lostSubscriberEventFinder <- LostSubscriberEventFinder(queriesExecTimes)
    zombieNodesCleaner        <- ZombieNodesCleaner(queriesExecTimes)
    lostZombieEventFinder     <- LostZombieEventFinder(queriesExecTimes)
  } yield new EventFinder[F](longProcessingEventFinder,
                             lostSubscriberEventFinder,
                             zombieNodesCleaner,
                             lostZombieEventFinder
  )
}

private trait ZombieEventSubProcess {
  val processName: ZombieEventProcess
}
