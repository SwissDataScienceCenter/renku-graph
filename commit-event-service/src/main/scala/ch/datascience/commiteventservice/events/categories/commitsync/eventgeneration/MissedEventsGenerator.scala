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

package ch.datascience.commiteventservice.events.categories.commitsync
package eventgeneration

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal.CommitToEventLog
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder, TokenRepositoryUrl}
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[commitsync] trait MissedEventsGenerator[Interpretation[_]] {
  def generateMissedEvents(commitSyncRequest: CommitSyncEvent): Interpretation[Unit]
}

private class MissedEventsGeneratorImpl(
    accessTokenFinder:     AccessTokenFinder[IO],
    latestCommitFinder:    LatestCommitFinder[IO],
    projectInfoFinder:     ProjectInfoFinder[IO],
    commitToEventLog:      CommitToEventLog[IO],
    logger:                Logger[IO],
    executionTimeRecorder: ExecutionTimeRecorder[IO]
)(implicit contextShift:   ContextShift[IO])
    extends MissedEventsGenerator[IO] {

  import IOAccessTokenFinder._
  import UpdateResult._
  import accessTokenFinder._
  import commitToEventLog._
  import executionTimeRecorder._
  import latestCommitFinder._
  import projectInfoFinder._

  def generateMissedEvents(event: CommitSyncEvent): IO[Unit] = measureExecutionTime {
    loadEvents(event)
  } flatMap logResult(event) recoverWith loggingError(event)

  private def logResult(event: CommitSyncEvent): ((ElapsedTime, UpdateResult)) => IO[Unit] = {
    case (elapsedTime, Skipped) =>
      logger.info(s"${logMessageCommon(event)} -> no new events found in ${elapsedTime}ms")
    case (elapsedTime, Updated) =>
      logger.info(s"${logMessageCommon(event)} -> new events found in ${elapsedTime}ms")
    case (elapsedTime, Failed(message, exception)) =>
      logger.error(exception)(s"${logMessageCommon(event)} -> $message in ${elapsedTime}ms")
  }

  private def loadEvents(event: CommitSyncEvent): IO[UpdateResult] = {
    for {
      maybeAccessToken  <- findAccessToken(event.project.path)
      maybeLatestCommit <- findLatestCommit(event.project.id, maybeAccessToken).value
      updateResult      <- addEventsIfMissing(event, maybeLatestCommit, maybeAccessToken)
    } yield updateResult
  } recoverWith toUpdateResult

  private def addEventsIfMissing(event:             CommitSyncEvent,
                                 maybeLatestCommit: Option[CommitInfo],
                                 maybeAccessToken:  Option[AccessToken]
  ) = maybeLatestCommit -> event match {
    case (None, _)                                                                => Skipped.pure[IO]
    case (Some(commitInfo), FullCommitSyncEvent(id, _, _)) if commitInfo.id == id => Skipped.pure[IO]
    case (Some(commitInfo), syncEvent) =>
      for {
        projectInfo <- findProjectInfo(syncEvent.project.id, maybeAccessToken)
        startCommit <- startCommitFrom(commitInfo, projectInfo)
        _           <- storeCommitsInEventLog(startCommit)
      } yield Updated
  }

  private def startCommitFrom(commitInfo: CommitInfo, projectInfo: ProjectInfo) = StartCommit(
    id = commitInfo.id,
    project = Project(projectInfo.id, projectInfo.path)
  ).pure[IO]

  private lazy val toUpdateResult: PartialFunction[Throwable, IO[UpdateResult]] = { case NonFatal(exception) =>
    Failed("synchronization failed", exception).pure[IO]
  }

  private def loggingError(event: CommitSyncEvent): PartialFunction[Throwable, IO[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)(s"${logMessageCommon(event)} -> synchronization failed")
    exception.raiseError[IO, Unit]
  }

  private sealed trait UpdateResult extends Product with Serializable
  private object UpdateResult {
    final case object Skipped extends UpdateResult
    final case object Updated extends UpdateResult
    case class Failed(message: String, exception: Throwable) extends UpdateResult
  }
}

private[commitsync] object MissedEventsGenerator {
  def apply(
      gitLabThrottler:       Throttler[IO, GitLab],
      executionTimeRecorder: ExecutionTimeRecorder[IO],
      logger:                Logger[IO]
  )(implicit
      timer:            Timer[IO],
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext
  ): IO[MissedEventsGenerator[IO]] =
    for {
      tokenRepositoryUrl <- TokenRepositoryUrl[IO]()
      gitLabUrl          <- GitLabUrl[IO]()
      commitToEventLog   <- CommitToEventLog(gitLabThrottler, executionTimeRecorder, logger)
      latestCommitFinder <- LatestCommitFinder(gitLabThrottler, logger)
    } yield new MissedEventsGeneratorImpl(
      new IOAccessTokenFinder(tokenRepositoryUrl, logger),
      latestCommitFinder,
      new ProjectInfoFinderImpl(gitLabUrl, gitLabThrottler, logger),
      commitToEventLog,
      logger,
      executionTimeRecorder
    )
}
