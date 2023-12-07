/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.commiteventservice.events.consumers.commitsync.eventgeneration

import cats.MonadThrow
import cats.data.StateT
import cats.effect.Async
import cats.syntax.all._
import io.renku.commiteventservice.events.consumers.commitsync._
import io.renku.commiteventservice.events.consumers.common.SynchronizationSummary._
import io.renku.commiteventservice.events.consumers.common.UpdateResult._
import io.renku.commiteventservice.events.consumers.common._
import io.renku.eventlog
import io.renku.eventlog.api.events.GlobalCommitSyncRequest
import io.renku.events.consumers.Project
import io.renku.graph.model.events.{BatchDate, CommitId}
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.logging.ExecutionTimeRecorder
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax._

import scala.util.control.NonFatal

private[commitsync] trait CommitsSynchronizer[F[_]] {
  def synchronizeEvents(event: CommitSyncEvent): F[Unit]
}

private[commitsync] class CommitsSynchronizerImpl[F[_]: MonadThrow: Logger: AccessTokenFinder: ExecutionTimeRecorder](
    latestCommitFinder:  LatestCommitFinder[F],
    eventDetailsFinder:  EventDetailsFinder[F],
    commitInfoFinder:    CommitInfoFinder[F],
    commitToEventLog:    CommitToEventLog[F],
    commitEventsRemover: CommitEventsRemover[F],
    elClient:            eventlog.api.events.Client[F],
    clock:               java.time.Clock = java.time.Clock.systemUTC()
) extends CommitsSynchronizer[F] {

  private val accessTokenFinder: AccessTokenFinder[F] = AccessTokenFinder[F]
  import accessTokenFinder._
  import commitEventsRemover._
  import commitInfoFinder._
  import commitToEventLog._
  import eventDetailsFinder._
  private val executionTimeRecorder = ExecutionTimeRecorder[F]
  import executionTimeRecorder._
  import latestCommitFinder._

  override def synchronizeEvents(event: CommitSyncEvent): F[Unit] = {
    (Logger[F].info(show"$categoryName: $event accepted") >>
      findAccessToken(event.project.id)) >>= {
      case None =>
        info"${logMessageCommon(event)} -> No access token found sending GlobalCommitSyncRequest" >>
          triggerGlobalCommitSync(event)
      case implicit0(mat: Option[AccessToken]) =>
        findLatestCommit(event.project.id) >>= (checkForSkippedEvent(_, event))
    }
  } recoverWith loggingError(event, "Synchronization failed")

  private def checkForSkippedEvent(maybeLatestCommit: Option[CommitInfo], event: CommitSyncEvent)(implicit
      maybeAccessToken: Option[AccessToken]
  ): F[Unit] = (maybeLatestCommit, event) match {
    case (Some(commitInfo), FullCommitSyncEvent(id, _, _)) if commitInfo.id == id =>
      measureExecutionTime(Skipped.pure[F].widen[UpdateResult]) >>= logResult(event)
    case (Some(commitInfo), event) =>
      processCommitsAndLogSummary(commitInfo.id, event)
    case (None, FullCommitSyncEvent(id, _, _)) =>
      processCommitsAndLogSummary(id, event)
    case (None, MinimalCommitSyncEvent(_)) =>
      measureExecutionTime(Skipped.pure[F].widen[UpdateResult]) >>= logResult(event)
  }

  private def processCommitsAndLogSummary(commitId: CommitId, event: CommitSyncEvent)(implicit
      maybeAccessToken: Option[AccessToken]
  ) = measureExecutionTime(
    processCommits(List(commitId), event.project, BatchDate(clock)).run(SynchronizationSummary())
  ) flatTap { case (_, summary) =>
    if (summary._2.get(Created) > 0 || summary._2.get(Deleted) > 0) {
      triggerGlobalCommitSync(event) recoverWith loggingErrorAndIgnoreError(event,
                                                                            "Triggering Global Commit Sync Failed"
      )
    } else ().pure[F]
  } flatMap { case (elapsedTime: ElapsedTime, summary) =>
    logSummary(commitId, event.project)(elapsedTime, summary._2)
  }

  private def triggerGlobalCommitSync(event: CommitSyncEvent): F[Unit] =
    elClient.send(GlobalCommitSyncRequest(event.project))

  private val DontCareCommitId = CommitId("0000000000000000000000000000000000000000")

  private def processCommits(commitList: List[CommitId], project: Project, batchDate: BatchDate)(implicit
      maybeToken: Option[AccessToken]
  ): SummaryState[F, SynchronizationSummary] = commitList match {
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
              _               <- incrementCount[F](result)
              _               <- StateT.liftF(logResult(commitId, project)(elapsedTime -> result))
              continueProcess <- processCommits(commits, project, batchDate)
            } yield continueProcess
        }
    case Nil => StateT.get[F, SynchronizationSummary]
  }

  private def collectResult(result:    UpdateResult,
                            commitId:  CommitId,
                            commitIds: List[CommitId]
  ): F[(UpdateResult, CommitId, List[CommitId])] = (result, commitId, commitIds).pure[F]

  private def sendDeletionStatusAndRecover(project: Project, commitFromEL: CommitWithParents): F[UpdateResult] =
    removeDeletedEvent(project, commitFromEL.id).recoverWith { case NonFatal(e) =>
      Failed(s"$categoryName: Commit Remover failed to send commit deletion status", e)
        .pure[F]
        .widen[UpdateResult]
    }

  private def getInfoFromELandGL(commitId: CommitId, project: Project)(implicit
      maybeAccessToken: Option[AccessToken]
  ): F[(Option[CommitWithParents], Option[CommitInfo])] = for {
    maybeEventDetailsFromEL <- getEventDetails(project.id, commitId)
    maybeInfoFromGL         <- getMaybeCommitInfo(project.id, commitId)
  } yield (maybeEventDetailsFromEL, maybeInfoFromGL)

  private def loggingErrorAndIgnoreError(event:        CommitSyncEvent,
                                         errorMessage: String
  ): PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(s"${logMessageCommon(event)} -> $errorMessage")
  }

  private def loggingError(event: CommitSyncEvent, errorMessage: String): PartialFunction[Throwable, F[Unit]] = {
    case NonFatal(exception) =>
      Logger[F]
        .error(exception)(s"${logMessageCommon(event)} -> $errorMessage")
        .flatMap(_ => exception.raiseError[F, Unit])
  }

  private def logResult(event: CommitSyncEvent): ((ElapsedTime, UpdateResult)) => F[Unit] = {
    case (_, Skipped) => ().pure[F]
    case (_, Existed) => ().pure[F]
    case (elapsedTime, Created) =>
      Logger[F].info(s"${logMessageCommon(event)} -> new events found in ${elapsedTime}ms")
    case (elapsedTime, Deleted) =>
      Logger[F].info(s"${logMessageCommon(event)} -> events found for deletion in ${elapsedTime}ms")
    case (elapsedTime, Failed(message, exception)) =>
      Logger[F].error(exception)(s"${logMessageCommon(event)} -> $message in ${elapsedTime}ms")
  }

  private def logResult(eventId: CommitId, project: Project): ((ElapsedTime, UpdateResult)) => F[Unit] = {
    case (_, Skipped) => ().pure[F]
    case (_, Existed) => ().pure[F]
    case (elapsedTime, Created) =>
      Logger[F].info(logMessageFor(eventId, project, s"new events found in ${elapsedTime}ms"))
    case (elapsedTime, Deleted) =>
      Logger[F].info(
        logMessageFor(eventId, project, s"events found for deletion in ${elapsedTime}ms")
      )
    case (elapsedTime, Failed(message, exception)) =>
      Logger[F].error(exception)(logMessageFor(eventId, project, s"$message in ${elapsedTime}ms"))
  }

  private def logSummary(eventId: CommitId, project: Project): ((ElapsedTime, SynchronizationSummary)) => F[Unit] = {
    case (elapsedTime, summary) =>
      Logger[F].info(
        logMessageFor(
          eventId,
          project,
          show"events generation result: $summary in ${elapsedTime}ms"
        )
      )
  }

  private def logMessageFor(eventId: CommitId, project: Project, message: String) =
    s"$categoryName: id = $eventId, projectId = ${project.id}, projectSlug = ${project.slug} -> $message"
}

private[commitsync] object CommitsSynchronizer {
  def apply[F[_]: Async: GitLabClient: AccessTokenFinder: Logger: MetricsRegistry: ExecutionTimeRecorder]
      : F[CommitsSynchronizerImpl[F]] = for {
    latestCommitFinder  <- LatestCommitFinder[F]
    eventDetailsFinder  <- EventDetailsFinder[F]
    commitInfoFinder    <- CommitInfoFinder[F]
    commitToEventLog    <- CommitToEventLog[F]
    commitEventsRemover <- CommitEventsRemover[F]
    elClient            <- eventlog.api.events.Client[F]
  } yield new CommitsSynchronizerImpl[F](latestCommitFinder,
                                         eventDetailsFinder,
                                         commitInfoFinder,
                                         commitToEventLog,
                                         commitEventsRemover,
                                         elClient
  )
}
