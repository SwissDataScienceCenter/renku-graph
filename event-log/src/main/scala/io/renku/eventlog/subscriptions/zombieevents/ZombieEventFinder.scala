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

import cats.MonadError
import cats.data.OptionT
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.subscriptions.EventFinder

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private class ZombieEventFinder[Interpretation[_]: MonadError[*[_], Throwable]](
    longProcessingEventsFinder: EventFinder[Interpretation, ZombieEvent],
    lostSubscriberEventFinder:  EventFinder[Interpretation, ZombieEvent],
    zombieEventSourceCleaner:   ZombieEventSourceCleaner[Interpretation],
    lostZombieEventFinder:      EventFinder[Interpretation, ZombieEvent],
    logger:                     Logger[Interpretation]
) extends EventFinder[Interpretation, ZombieEvent] {
  override def popEvent(): Interpretation[Option[ZombieEvent]] = for {
    _ <- zombieEventSourceCleaner.removeZombieSources() recoverWith logError
    maybeEvent <- OptionT(longProcessingEventsFinder.popEvent())
                    .orElseF(lostSubscriberEventFinder.popEvent())
                    .orElseF(lostZombieEventFinder.popEvent())
                    .value
  } yield maybeEvent

  private lazy val logError: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(e) =>
    logger.error(e)("ZombieEventSourceCleaner - failure during clean up")
  }
}

private object ZombieEventFinder {

  def apply(
      transactor:       DbTransactor[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
      logger:           Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventFinder[IO, ZombieEvent]] = for {
    longProcessingEventFinder <- LongProcessingEventFinder(transactor, queriesExecTimes)
    lostSubscriberEventFinder <- LostSubscriberEventFinder(transactor, queriesExecTimes)
    zombieEventSourceCleaner  <- ZombieEventSourceCleaner(transactor, queriesExecTimes, logger)
    lostZombieEventFinder     <- LostZombieEventFinder(transactor, queriesExecTimes)
  } yield new ZombieEventFinder[IO](longProcessingEventFinder,
                                    lostSubscriberEventFinder,
                                    zombieEventSourceCleaner,
                                    lostZombieEventFinder,
                                    logger
  )
}

private trait ZombieEventSubProcess {
  val processName: ZombieEventProcess
}
