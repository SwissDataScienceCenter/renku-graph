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

package io.renku.commiteventservice.events.categories.commitsync.eventgeneration

import cats.MonadThrow
import cats.data.StateT
import cats.effect.Async
import cats.effect.kernel.Temporal
import cats.syntax.all._
import io.renku.commiteventservice.events.categories.commitsync._
import io.renku.commiteventservice.events.categories.common.SynchronizationSummary._
import io.renku.commiteventservice.events.categories.common.UpdateResult._
import io.renku.commiteventservice.events.categories.common._
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.events.consumers.Project
import io.renku.graph.model.events.{BatchDate, CommitId}
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.graph.tokenrepository.AccessTokenFinder._
import io.renku.http.client.AccessToken
import io.renku.logging.ExecutionTimeRecorder
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private[commitsync] trait CommitEventSynchronizer[Interpretation[_]] {
  def synchronizeEvents(event: CommitSyncEvent): Interpretation[Unit]
}

private[commitsync] class CommitEventSynchronizerImpl[Interpretation[_]: MonadThrow: Logger](
    accessTokenFinder:     AccessTokenFinder[Interpretation],
    latestCommitFinder:    LatestCommitFinder[Interpretation],
    eventDetailsFinder:    EventDetailsFinder[Interpretation],
    commitInfoFinder:      CommitInfoFinder[Interpretation],
    commitToEventLog:      CommitToEventLog[Interpretation],
    commitEventsRemover:   CommitEventsRemover[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    clock:                 java.time.Clock = java.time.Clock.systemUTC()
) extends CommitEventSynchronizer[Interpretation] {

  import accessTokenFinder._
  import commitEventsRemover._
  import commitInfoFinder._
  import commitToEventLog._
  import eventDetailsFinder._
  import executionTimeRecorder._
  import latestCommitFinder._

  override def synchronizeEvents(event: CommitSyncEvent): Interpretation[Unit] = (for {
    maybeAccessToken  <- findAccessToken(event.project.id)
    maybeLatestCommit <- findLatestCommit(event.project.id, maybeAccessToken).value
    _                 <- checkForSkippedEvent(maybeLatestCommit, event)(maybeAccessToken)
  } yield ()) recoverWith loggingError(event)

  private def checkForSkippedEvent(maybeLatestCommit: Option[CommitInfo], event: CommitSyncEvent)(implicit
      maybeAccessToken:                               Option[AccessToken]
  ) = (maybeLatestCommit, event) match {
    case (Some(commitInfo), FullCommitSyncEvent(id, _, _)) if commitInfo.id == id =>
      measureExecutionTime(Skipped.pure[Interpretation].widen[UpdateResult]) >>= logResult(event)
    case (Some(commitInfo), event) =>
      processCommitsAndLogSummary(commitInfo.id, event.project)
    case (None, FullCommitSyncEvent(id, _, _)) =>
      processCommitsAndLogSummary(id, event.project)
    case (None, MinimalCommitSyncEvent(_)) =>
      measureExecutionTime(Skipped.pure[Interpretation].widen[UpdateResult]) >>= logResult(event)
  }

  private def processCommitsAndLogSummary(commitId: CommitId, project: Project)(implicit
      maybeAccessToken:                             Option[AccessToken]
  ) = measureExecutionTime(
    processCommits(List(commitId), project, BatchDate(clock)).run(SynchronizationSummary())
  ) flatMap { case (elapsedTime: ElapsedTime, summary) =>
    logSummary(commitId, project)(elapsedTime, summary._2)
  }

  private val DontCareCommitId = CommitId("0000000000000000000000000000000000000000")

  private def processCommits(commitList: List[CommitId], project: Project, batchDate: BatchDate)(implicit
      maybeToken:                        Option[AccessToken]
  ): SummaryState[Interpretation, SynchronizationSummary] = commitList match {
    case commitId :: commitIds =>
      StateT
        .liftF(
          measureExecutionTime(
            getInfoFromELandGL(commitId, project)
              .flatMap {
                case (None, Some(CommitInfo(DontCareCommitId, _, _, _, _, _))) =>
                  collectResult(Skipped, commitId, commitIds)
                case (Some(commitFromEL), Some(CommitInfo(DontCareCommitId, _, _, _, _, _)) | None) =>
                  sendDeletionStatusAndRecover(project, commitFromEL)
                    .map((_, commitFromEL.id, commitFromEL.parents.filterNot(_ == DontCareCommitId) ::: commitIds))
                case (Some(_), Some(_)) => collectResult(Existed, commitId, Nil)
                case (None, Some(commitFromGL)) =>
                  storeCommitInEventLog(project, commitFromGL, batchDate)
                    .map((_, commitId, commitFromGL.parents.filterNot(_ == DontCareCommitId) ::: commitIds))
                case _ => collectResult(Skipped, commitId, Nil)
              }
              .recoverWith { case NonFatal(error) =>
                collectResult(Failed(s"Synchronization failed", error), commitId, commitIds)
              }
          )
        )
        .flatMap {
          case (elapsedTime: ElapsedTime, (result: UpdateResult, commitId: CommitId, commits: List[CommitId])) =>
            for {
              _               <- incrementCount[Interpretation](result)
              _               <- StateT.liftF(logResult(commitId, project)(elapsedTime -> result))
              continueProcess <- processCommits(commits, project, batchDate)
            } yield continueProcess
        }
    case Nil => StateT.get[Interpretation, SynchronizationSummary]
  }

  private def collectResult(result:    UpdateResult,
                            commitId:  CommitId,
                            commitIds: List[CommitId]
  ): Interpretation[(UpdateResult, CommitId, List[CommitId])] = (result, commitId, commitIds).pure[Interpretation]

  private def sendDeletionStatusAndRecover(project:      Project,
                                           commitFromEL: CommitWithParents
  ): Interpretation[UpdateResult] =
    removeDeletedEvent(project, commitFromEL.id).recoverWith { case NonFatal(e) =>
      Failed(s"$categoryName - Commit Remover failed to send commit deletion status", e)
        .pure[Interpretation]
        .widen[UpdateResult]
    }

  private def getInfoFromELandGL(commitId: CommitId, project: Project)(implicit
      maybeAccessToken:                    Option[AccessToken]
  ): Interpretation[(Option[CommitWithParents], Option[CommitInfo])] = for {
    maybeEventDetailsFromEL <- getEventDetails(project.id, commitId)
    maybeInfoFromGL         <- getMaybeCommitInfo(project.id, commitId)
  } yield (maybeEventDetailsFromEL, maybeInfoFromGL)

  private def loggingError(event: CommitSyncEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      Logger[Interpretation]
        .error(exception)(s"${logMessageCommon(event)} -> Synchronization failed")
        .flatMap(_ => exception.raiseError[Interpretation, Unit])
  }

  private def logResult(event: CommitSyncEvent): ((ElapsedTime, UpdateResult)) => Interpretation[Unit] = {
    case (elapsedTime, Skipped) =>
      Logger[Interpretation].info(s"${logMessageCommon(event)} -> event skipped in ${elapsedTime}ms")
    case (elapsedTime, Existed) =>
      Logger[Interpretation].info(s"${logMessageCommon(event)} -> no new event found in ${elapsedTime}ms")
    case (elapsedTime, Created) =>
      Logger[Interpretation].info(s"${logMessageCommon(event)} -> new events found in ${elapsedTime}ms")
    case (elapsedTime, Deleted) =>
      Logger[Interpretation].info(s"${logMessageCommon(event)} -> events found for deletion in ${elapsedTime}ms")
    case (elapsedTime, Failed(message, exception)) =>
      Logger[Interpretation].error(exception)(s"${logMessageCommon(event)} -> $message in ${elapsedTime}ms")
  }

  private def logResult(eventId: CommitId, project: Project): ((ElapsedTime, UpdateResult)) => Interpretation[Unit] = {
    case (elapsedTime, Skipped) =>
      Logger[Interpretation].info(logMessageFor(eventId, project, s"event skipped in ${elapsedTime}ms"))
    case (elapsedTime, Existed) =>
      Logger[Interpretation].info(logMessageFor(eventId, project, s"no new events found in ${elapsedTime}ms"))
    case (elapsedTime, Created) =>
      Logger[Interpretation].info(logMessageFor(eventId, project, s"new events found in ${elapsedTime}ms"))
    case (elapsedTime, Deleted) =>
      Logger[Interpretation].info(
        logMessageFor(eventId, project, s"events found for deletion in ${elapsedTime}ms")
      )
    case (elapsedTime, Failed(message, exception)) =>
      Logger[Interpretation].error(exception)(logMessageFor(eventId, project, s"$message in ${elapsedTime}ms"))
  }

  private def logSummary(eventId: CommitId,
                         project: Project
  ): ((ElapsedTime, SynchronizationSummary)) => Interpretation[Unit] = { case (elapsedTime, summary) =>
    Logger[Interpretation].info(
      logMessageFor(
        eventId,
        project,
        show"events generation result: $summary in ${elapsedTime}ms"
      )
    )
  }

  private def logMessageFor(eventId: CommitId, project: Project, message: String) =
    s"$categoryName: id = $eventId, projectId = ${project.id}, projectPath = ${project.path} -> $message"
}

private[commitsync] object CommitEventSynchronizer {
  def apply[Interpretation[_]: Async: Temporal: Logger](gitLabThrottler: Throttler[Interpretation, GitLab],
                                                        executionTimeRecorder: ExecutionTimeRecorder[Interpretation]
  ) = for {
    accessTokenFinder   <- AccessTokenFinder[Interpretation]
    latestCommitFinder  <- LatestCommitFinder(gitLabThrottler)
    eventDetailsFinder  <- EventDetailsFinder[Interpretation]
    commitInfoFinder    <- CommitInfoFinder(gitLabThrottler)
    commitToEventLog    <- CommitToEventLog[Interpretation]
    commitEventsRemover <- CommitEventsRemover[Interpretation]
  } yield new CommitEventSynchronizerImpl[Interpretation](accessTokenFinder,
                                                          latestCommitFinder,
                                                          eventDetailsFinder,
                                                          commitInfoFinder,
                                                          commitToEventLog,
                                                          commitEventsRemover,
                                                          executionTimeRecorder
  )
}
