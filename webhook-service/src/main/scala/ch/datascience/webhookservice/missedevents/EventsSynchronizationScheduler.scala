/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.missedevents

import cats.MonadError
import cats.effect._
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.logging.ExecutionTimeRecorder
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

class EventsSynchronizationScheduler[Interpretation[_]](
    schedulerConfigProvider: SchedulerConfigProvider[Interpretation],
    eventsLoader:            MissedEventsLoader[Interpretation],
    logger:                  Logger[Interpretation]
)(implicit ME:               MonadError[Interpretation, Throwable], timer: Timer[Interpretation]) {

  import eventsLoader._
  import schedulerConfigProvider._

  def run(): Interpretation[Unit] =
    for {
      initialDelay <- getInitialDelay()
      interval     <- getInterval()
      _            <- timer sleep initialDelay
      _            <- loadEvents(interval).foreverM[Unit]
    } yield ()

  private def loadEvents(interval: FiniteDuration): Interpretation[Unit] = {
    for {
      _ <- timer sleep interval
      _ <- loadMissedEvents
    } yield ()
  } recoverWith sleepAndRetry

  private lazy val sleepAndRetry: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)("Event synchronization failed to synchronize events")
  }
}

object IOEventsSynchronizationScheduler {
  def apply(
      gitLabThrottler:       Throttler[IO, GitLab],
      executionTimeRecorder: ExecutionTimeRecorder[IO],
      logger:                Logger[IO]
  )(implicit
      timer:            Timer[IO],
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext
  ): IO[EventsSynchronizationScheduler[IO]] =
    for {
      missedEventsLoader <- IOMissedEventsLoader(gitLabThrottler, executionTimeRecorder, logger)
    } yield new EventsSynchronizationScheduler[IO](new SchedulerConfigProvider[IO](), missedEventsLoader, logger)
}
