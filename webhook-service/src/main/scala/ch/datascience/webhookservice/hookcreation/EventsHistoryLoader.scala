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

package ch.datascience.webhookservice.hookcreation

import cats.MonadError
import cats.data.OptionT
import cats.effect.{Clock, ContextShift, IO}
import cats.implicits._
import ch.datascience.control.Throttler
import ch.datascience.graph.gitlab.GitLab
import ch.datascience.graph.model.events.Project
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ApplicationLogger
import ch.datascience.webhookservice.config.GitLabConfigProvider
import ch.datascience.webhookservice.eventprocessing.PushEvent
import ch.datascience.webhookservice.eventprocessing.pushevent.{IOPushEventSender, PushEventSender}
import ch.datascience.webhookservice.project.ProjectInfo
import ch.datascience.webhookservice.pushevents.LatestPushEventFetcher.PushEventInfo
import ch.datascience.webhookservice.pushevents._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

private class EventsHistoryLoader[Interpretation[_]](
    latestPushEventFetcher: LatestPushEventFetcher[Interpretation],
    pushEventSender:        PushEventSender[Interpretation],
    logger:                 Logger[Interpretation]
)(implicit ME:              MonadError[Interpretation, Throwable]) {

  import latestPushEventFetcher._
  import pushEventSender._

  def loadAllEvents(projectInfo: ProjectInfo, accessToken: AccessToken): Interpretation[Unit] = {
    for {
      latestPushEvent <- OptionT(fetchLatestPushEvent(projectInfo.id, Some(accessToken)))
      pushEvent       <- OptionT.liftF(pushEventFrom(latestPushEvent, projectInfo))
      _               <- OptionT.liftF(storeCommitsInEventLog(pushEvent))
    } yield ()
  }.value
    .flatMap(_ => ME.unit)
    .recoverWith(loggingError(projectInfo))

  private def pushEventFrom(pushEventInfo: PushEventInfo, projectInfo: ProjectInfo) = ME.pure {
    PushEvent(
      maybeCommitFrom = None,
      commitTo        = pushEventInfo.commitTo,
      pushUser        = pushEventInfo.pushUser,
      project         = Project(projectInfo.id, projectInfo.path)
    )
  }

  private def loggingError(projectInfo: ProjectInfo): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"Project: ${projectInfo.id}: Sending events to the Event Log failed")
      ME.raiseError(exception)
  }
}

private class IOEventsHistoryLoader(
    gitLabThrottler: Throttler[IO, GitLab]
)(
    implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    clock:                     Clock[IO]
) extends EventsHistoryLoader[IO](
      new IOLatestPushEventFetcher(new GitLabConfigProvider[IO], gitLabThrottler),
      new IOPushEventSender(gitLabThrottler),
      ApplicationLogger
    )
