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

import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.dbeventlog.commands.EventLogLatestEvents
import ch.datascience.graph.model.events.{CommitEventId, Project}
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.webhookservice.commits.{CommitInfo, LatestCommitFinder}
import ch.datascience.webhookservice.eventprocessing.StartCommit
import ch.datascience.webhookservice.eventprocessing.startcommit.CommitToEventLog
import ch.datascience.webhookservice.project.{ProjectInfo, ProjectInfoFinder}
import io.chrisdavenport.log4cats.Logger

import scala.language.higherKinds
import scala.util.control.NonFatal

private abstract class MissedEventsLoader[Interpretation[_]] {
  def loadMissedEvents: Interpretation[Unit]
}

private class IOMissedEventsLoader(
    eventLogLatestEvents:  EventLogLatestEvents[IO],
    accessTokenFinder:     AccessTokenFinder[IO],
    latestCommitFinder:    LatestCommitFinder[IO],
    projectInfoFinder:     ProjectInfoFinder[IO],
    commitToEventLog:      CommitToEventLog[IO],
    logger:                Logger[IO],
    executionTimeRecorder: ExecutionTimeRecorder[IO]
)(implicit contextShift:   ContextShift[IO])
    extends MissedEventsLoader[IO] {

  import UpdateResult._
  import accessTokenFinder._
  import commitToEventLog._
  import eventLogLatestEvents._
  import executionTimeRecorder._
  import latestCommitFinder._
  import projectInfoFinder._

  def loadMissedEvents: IO[Unit] =
    measureExecutionTime {
      for {
        latestLogEvents <- findAllLatestEvents
        updateSummary <- if (latestLogEvents.isEmpty) IO.pure(UpdateSummary())
                        else (latestLogEvents map loadEvents).sequence map toUpdateSummary
      } yield updateSummary
    } flatMap logSummary recoverWith loggingError

  private lazy val logSummary: ((ElapsedTime, UpdateSummary)) => IO[Unit] = {
    case (elapsedTime, updateSummary) =>
      logger.info(
        s"Synchronized Commits with GitLab in ${elapsedTime}ms: " +
          s"${updateSummary(Updated)} updates, " +
          s"${updateSummary(Skipped)} skipped, " +
          s"${updateSummary(Failed)} failed"
      )
  }

  private def loadEvents(latestLogEvent: CommitEventId): IO[UpdateResult] = {
    for {
      maybeAccessToken  <- findAccessToken(latestLogEvent.projectId)
      maybeLatestCommit <- findLatestCommit(latestLogEvent.projectId, maybeAccessToken).value
      updateResult      <- addEventsIfMissing(latestLogEvent, maybeLatestCommit, maybeAccessToken)
    } yield updateResult
  } recoverWith loggingWarning(latestLogEvent)

  private def addEventsIfMissing(latestLogEvent:    CommitEventId,
                                 maybeLatestCommit: Option[CommitInfo],
                                 maybeAccessToken:  Option[AccessToken]) =
    maybeLatestCommit match {
      case None                                                   => IO.pure(Skipped)
      case Some(commitInfo) if commitInfo.id == latestLogEvent.id => IO.pure(Skipped)
      case Some(commitInfo) =>
        for {
          projectInfo <- findProjectInfo(latestLogEvent.projectId, maybeAccessToken)
          startCommit <- startCommitFrom(commitInfo, projectInfo)
          _           <- storeCommitsInEventLog(startCommit)
        } yield Updated
    }

  private def startCommitFrom(commitInfo: CommitInfo, projectInfo: ProjectInfo) = IO.pure {
    StartCommit(
      id      = commitInfo.id,
      project = Project(projectInfo.id, projectInfo.path)
    )
  }

  private def loggingWarning(latestLogEvent: CommitEventId): PartialFunction[Throwable, IO[UpdateResult]] = {
    case NonFatal(exception) =>
      logger.warn(exception)(s"Synchronizing Commits for project ${latestLogEvent.projectId} failed")
      IO.pure(Failed)
  }

  private lazy val loggingError: PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)("Synchronizing Commits with GitLab failed")
      IO.raiseError(exception)
  }

  private sealed trait UpdateResult extends Product with Serializable
  private object UpdateResult {
    final case object Skipped extends UpdateResult
    final case object Updated extends UpdateResult
    final case object Failed  extends UpdateResult
  }

  private case class UpdateSummary(state: Map[UpdateResult, Int] = Map.empty) {
    def apply(updateResult: UpdateResult): Int = state.getOrElse(updateResult, 0)
  }

  private def toUpdateSummary(updateResults: List[UpdateResult]) = UpdateSummary(
    updateResults.groupBy(identity).mapValues(_.size)
  )
}
