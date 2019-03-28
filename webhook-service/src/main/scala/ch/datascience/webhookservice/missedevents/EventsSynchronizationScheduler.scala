/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import cats.implicits._
import ch.datascience.control.Throttler
import ch.datascience.dbeventlog.commands.IOEventLogLatestEvents
import ch.datascience.graph.gitlab.GitLab
import ch.datascience.graph.tokenrepository.{IOAccessTokenFinder, TokenRepositoryUrlProvider}
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.webhookservice.config.GitLabConfigProvider
import ch.datascience.webhookservice.eventprocessing.pushevent.IOPushEventSender
import ch.datascience.webhookservice.project.IOProjectInfoFinder
import ch.datascience.webhookservice.pushevents.IOLatestPushEventFetcher

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import scala.util.control.NonFatal

class EventsSynchronizationScheduler[Interpretation[_]](
    schedulerConfigProvider: SchedulerConfigProvider[Interpretation],
    eventsLoader:            MissedEventsLoader[Interpretation]
)(implicit ME:               MonadError[Interpretation, Throwable], timer: Timer[Interpretation]) {

  import eventsLoader._
  import schedulerConfigProvider._

  def run: Interpretation[Unit] =
    for {
      initialDelay <- getInitialDelay
      interval     <- getInterval
      _            <- timer sleep initialDelay
      _            <- loadEvents(interval)
    } yield ()

  private def loadEvents(interval: FiniteDuration): Interpretation[Unit] = {
    for {
      _ <- loadMissedEvents
      _ <- timer sleep interval
      _ <- loadEvents(interval)
    } yield ()
  } recoverWith sleepAndRetry(interval)

  private def sleepAndRetry(interval: FiniteDuration): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(_) =>
      for {
        _ <- timer sleep interval
        _ <- loadEvents(interval)
      } yield ()
  }
}

class IOEventsSynchronizationScheduler(
    gitLabThrottler: Throttler[IO, GitLab]
)(implicit timer:    Timer[IO], contextShift: ContextShift[IO], executionContext: ExecutionContext)
    extends EventsSynchronizationScheduler[IO](
      new SchedulerConfigProvider[IO](),
      new IOMissedEventsLoader(
        new IOEventLogLatestEvents,
        new IOAccessTokenFinder(new TokenRepositoryUrlProvider[IO]()),
        new IOLatestPushEventFetcher(new GitLabConfigProvider[IO], gitLabThrottler),
        new IOProjectInfoFinder(new GitLabConfigProvider[IO], gitLabThrottler),
        new IOPushEventSender(gitLabThrottler),
        ApplicationLogger,
        new ExecutionTimeRecorder[IO]
      )
    )
