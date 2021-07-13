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

package ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration

import cats.{Applicative, MonadThrow}
import cats.data.StateT
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.SynchronizationSummary
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.SynchronizationSummary.{SummaryKey, SummaryState, add}
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal.EventDetailsFinder
import ch.datascience.commiteventservice.events.categories.common.UpdateResult._
import ch.datascience.commiteventservice.events.categories.common.{CommitInfo, CommitInfoFinder, CommitToEventLog, CommitWithParents, EventStatusPatcher, UpdateResult}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.events.{BatchDate, CommitId}
import ch.datascience.graph.model.projects
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.graph.tokenrepository.AccessTokenFinder._
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
private[commitsync] trait CommitEventSynchronizer[Interpretation[_]] {
  def synchronizeEvents(event: CommitSyncEvent): Interpretation[Unit]
}

private[commitsync] class CommitEventSynchronizerImpl[Interpretation[_]: MonadThrow](
    accessTokenFinder:     AccessTokenFinder[Interpretation],
    latestCommitFinder:    LatestCommitFinder[Interpretation],
    eventDetailsFinder:    EventDetailsFinder[Interpretation],
    commitInfoFinder:      CommitInfoFinder[Interpretation],
    commitToEventLog:      CommitToEventLog[Interpretation],
    eventStatusPatcher:    EventStatusPatcher[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation],
    clock:                 java.time.Clock = java.time.Clock.systemUTC()
) extends CommitEventSynchronizer[Interpretation] {

  import accessTokenFinder._
  import commitInfoFinder._
  import commitToEventLog._
  import eventDetailsFinder._
  import eventStatusPatcher._
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
  ) =
    measureExecutionTime(
      processCommits(List(commitId), project, BatchDate(clock)).run(SynchronizationSummary())
    ) flatMap { case (elapsedTime: ElapsedTime, summary) =>
      logSummary(commitId, project)(elapsedTime, summary._2)
    }

  private val DontCareCommitId = CommitId("0000000000000000000000000000000000000000")

  private def processCommits(commitList: List[CommitId], project: Project, batchDate: BatchDate)(implicit
      maybeToken:                        Option[AccessToken]
  ): SummaryState[Interpretation, SynchronizationSummary] =
    commitList match {
      case commitId :: commitIds =>
        StateT
          .liftF(
            measureExecutionTime(
              getInfoFromELandGL(commitId, project)
                .flatMap {
                  case (_, Some(CommitInfo(DontCareCommitId, _, _, _, _, _))) =>
                    collectResult(Skipped, commitId, commitIds)
                  case (Some(_), Some(_)) =>
                    collectResult(Existed, commitId, commitIds)
                  case (None, Some(commitFromGL)) =>
                    storeCommitInEventLog(project, commitFromGL, batchDate)
                      .map(result =>
                        (result, commitId, commitIds ++: commitFromGL.parents.filterNot(_ == DontCareCommitId))
                      )
                  case (Some(commitFromEL), None) =>
                    sendDeletionStatusAndRecover(project.id, commitFromEL, commitId, commitIds)
                  case _ =>
                    collectResult(Skipped, commitId, commitIds)
                }
                .recoverWith { case NonFatal(error) =>
                  collectResult(Failed(s"Synchronization failed", error), commitId, commitIds)
                }
            )
          )
          .flatMap {
            case (elapsedTime: ElapsedTime, (result: UpdateResult, commitId: CommitId, commits: List[CommitId])) =>
              for {
                _               <- add[Interpretation](result)
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

  private def sendDeletionStatusAndRecover(projectId:    projects.Id,
                                           commitFromEL: CommitWithParents,
                                           commitId:     CommitId,
                                           commitIds:    List[CommitId]
  ): Interpretation[(UpdateResult, CommitId, List[CommitId])] =
    (sendDeletionStatus(projectId, commitFromEL.id).map(_ => Deleted: UpdateResult) recoverWith { case NonFatal(e) =>
      Failed(s"$categoryName - Commit Remover failed to send commit deletion status", e)
        .pure[Interpretation]
        .widen[UpdateResult]
    }).map((_, commitId, commitIds ++: commitFromEL.parents.filterNot(_ == DontCareCommitId)))

  private def getInfoFromELandGL(commitId: CommitId, project: Project)(implicit
      maybeAccessToken:                    Option[AccessToken]
  ): Interpretation[(Option[CommitWithParents], Option[CommitInfo])] =
    for {
      maybeEventDetailsFromEL <- getEventDetails(project.id, commitId)
      maybeInfoFromGL         <- getMaybeCommitInfo(project.id, commitId)
    } yield (maybeEventDetailsFromEL, maybeInfoFromGL)

  private def loggingError(event: CommitSyncEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger
        .error(exception)(s"${logMessageCommon(event)} -> Synchronization failed")
        .flatMap(_ => exception.raiseError[Interpretation, Unit])

  }

  private def logResult(event: CommitSyncEvent): ((ElapsedTime, UpdateResult)) => Interpretation[Unit] = {
    case (elapsedTime, Skipped) =>
      logger.info(s"${logMessageCommon(event)} -> event skipped in ${elapsedTime}ms")
    case (elapsedTime, Existed) =>
      logger.info(s"${logMessageCommon(event)} -> no new event found in ${elapsedTime}ms")
    case (elapsedTime, Created) =>
      logger.info(s"${logMessageCommon(event)} -> new events found in ${elapsedTime}ms")
    case (elapsedTime, Deleted) =>
      logger.info(s"${logMessageCommon(event)} -> events found for deletion in ${elapsedTime}ms")
    case (elapsedTime, Failed(message, exception)) =>
      logger.error(exception)(s"${logMessageCommon(event)} -> $message in ${elapsedTime}ms")
  }

  private def logResult(eventId: CommitId, project: Project): ((ElapsedTime, UpdateResult)) => Interpretation[Unit] = {
    case (elapsedTime, Skipped) =>
      logger.info(logMessageFor(eventId, project, s"event skipped in ${elapsedTime}ms"))
    case (elapsedTime, Existed) =>
      logger.info(logMessageFor(eventId, project, s"no new events found in ${elapsedTime}ms"))
    case (elapsedTime, Created) =>
      logger.info(logMessageFor(eventId, project, s"new events found in ${elapsedTime}ms"))
    case (elapsedTime, Deleted) =>
      logger.info(
        logMessageFor(eventId, project, s"events found for deletion in ${elapsedTime}ms")
      )
    case (elapsedTime, Failed(message, exception)) =>
      logger.error(exception)(logMessageFor(eventId, project, s"$message in ${elapsedTime}ms"))
  }

  private def logSummary(eventId: CommitId,
                         project: Project
  ): ((ElapsedTime, SynchronizationSummary)) => Interpretation[Unit] = { case (elapsedTime, summary) =>
    logger.info(
      logMessageFor(
        eventId,
        project,
        s"events generation result: ${summary.getSummary()} in ${elapsedTime}ms"
      )
    )

  }

  private def logMessageFor(eventId: CommitId, project: Project, message: String) =
    s"$categoryName: id = $eventId, projectId = ${project.id}, projectPath = ${project.path} -> $message"

}

private[commitsync] object CommitEventSynchronizer {
  def apply(gitLabThrottler:       Throttler[IO, GitLab],
            executionTimeRecorder: ExecutionTimeRecorder[IO],
            logger:                Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ) = for {
    accessTokenFinder  <- AccessTokenFinder(logger)
    latestCommitFinder <- LatestCommitFinder(gitLabThrottler, logger)
    eventDetailsFinder <- EventDetailsFinder(logger)
    commitInfoFinder   <- CommitInfoFinder(gitLabThrottler, logger)
    commitToEventLog   <- CommitToEventLog(logger)
    eventStatusPatcher <- EventStatusPatcher(logger)
  } yield new CommitEventSynchronizerImpl[IO](accessTokenFinder,
                                              latestCommitFinder,
                                              eventDetailsFinder,
                                              commitInfoFinder,
                                              commitToEventLog,
                                              eventStatusPatcher,
                                              executionTimeRecorder,
                                              logger
  )

  final class SynchronizationSummary(private val summary: Map[SummaryKey, Int]) {
    import SynchronizationSummary._
    def getSummary(): String =
      s"${get("Created")} created, ${get("Existed")} existed, ${get("Skipped")} skipped, ${get("Deleted")} deleted, ${get("Failed")} failed"

    def get(key: String) = summary.getOrElse(key, 0)

    def updated(result: UpdateResult, newValue: Int): SynchronizationSummary = {
      val newSummary = summary.updated(toSummaryKey(result), newValue)
      new SynchronizationSummary(newSummary)
    }
  }

  object SynchronizationSummary {

    def apply() = new SynchronizationSummary(Map.empty[SummaryKey, Int])

    type SummaryKey = String

    type SummaryState[F[_], A] = StateT[F, SynchronizationSummary, A]

    def add[F[_]: Applicative](result: UpdateResult): SummaryState[F, Unit] = StateT { summaryMap =>
      val currentCount = summaryMap.get(toSummaryKey(result))
      (summaryMap.updated(result, currentCount + 1), ()).pure[F]
    }

    def toSummaryKey(result: UpdateResult): SummaryKey = result match {
      case Failed(_, _) => "Failed"
      case s            => s.toString
    }
  }

}
