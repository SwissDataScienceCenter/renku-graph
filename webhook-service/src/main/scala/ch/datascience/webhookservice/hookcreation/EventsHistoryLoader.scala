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

package ch.datascience.webhookservice.hookcreation

import cats.MonadError
import cats.data.OptionT
import cats.effect._
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.webhookservice.commits._
import ch.datascience.webhookservice.eventprocessing.startcommit.{CommitToEventLog, IOCommitToEventLog}
import ch.datascience.webhookservice.eventprocessing.{Project, StartCommit}
import ch.datascience.webhookservice.project.ProjectInfo
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private class EventsHistoryLoader[Interpretation[_]](
    latestCommitFinder: LatestCommitFinder[Interpretation],
    commitToEventLog:   CommitToEventLog[Interpretation],
    logger:             Logger[Interpretation]
)(implicit ME:          MonadError[Interpretation, Throwable]) {

  import commitToEventLog._
  import latestCommitFinder._

  def loadAllEvents(projectInfo: ProjectInfo, accessToken: AccessToken): Interpretation[Unit] = {
    for {
      latestCommit <- findLatestCommit(projectInfo.id, Some(accessToken))
      startCommit  <- OptionT.some[Interpretation](startCommitFrom(latestCommit, projectInfo))
      _            <- OptionT.liftF(storeCommitsInEventLog(startCommit))
    } yield ()
  }.value
    .flatMap(_ => ME.unit)
    .recoverWith(loggingError(projectInfo))

  private def startCommitFrom(latestCommit: CommitInfo, projectInfo: ProjectInfo) = StartCommit(
    id = latestCommit.id,
    project = Project(projectInfo.id, projectInfo.path)
  )

  private def loggingError(projectInfo: ProjectInfo): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"Project: ${projectInfo.id}: Sending events to the Event Log failed")
      ME.raiseError(exception)
  }
}

private object IOEventsHistoryLoader {
  def apply(
      gitLabThrottler:       Throttler[IO, GitLab],
      executionTimeRecorder: ExecutionTimeRecorder[IO],
      logger:                Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      clock:            Clock[IO],
      timer:            Timer[IO]
  ): IO[EventsHistoryLoader[IO]] =
    for {
      commitToEventLog   <- IOCommitToEventLog(gitLabThrottler, executionTimeRecorder, logger)
      latestCommitFinder <- IOLatestCommitFinder(gitLabThrottler, logger)
    } yield new EventsHistoryLoader[IO](
      latestCommitFinder,
      commitToEventLog,
      logger
    )
}
