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

package io.renku.eventlog.subscriptions.zombieevents

import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.subscriptions.EventFinder
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private class ZombieEventFinder[F[_]: MonadThrow: Logger](
    longProcessingEventsFinder: EventFinder[F, ZombieEvent],
    lostSubscriberEventFinder:  EventFinder[F, ZombieEvent],
    zombieNodesCleaner:         ZombieNodesCleaner[F],
    lostZombieEventFinder:      EventFinder[F, ZombieEvent]
) extends EventFinder[F, ZombieEvent] {

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

private object ZombieEventFinder {

  def apply[F[_]: Async: Parallel: Logger](
      sessionResource:  SessionResource[F, EventLogDB],
      queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ): F[EventFinder[F, ZombieEvent]] = for {
    longProcessingEventFinder <- LongProcessingEventFinder(sessionResource, queriesExecTimes)
    lostSubscriberEventFinder <- LostSubscriberEventFinder(sessionResource, queriesExecTimes)
    zombieNodesCleaner        <- ZombieNodesCleaner(sessionResource, queriesExecTimes)
    lostZombieEventFinder     <- LostZombieEventFinder(sessionResource, queriesExecTimes)
  } yield new ZombieEventFinder[F](longProcessingEventFinder,
                                   lostSubscriberEventFinder,
                                   zombieNodesCleaner,
                                   lostZombieEventFinder
  )
}

private trait ZombieEventSubProcess {
  val processName: ZombieEventProcess
}
