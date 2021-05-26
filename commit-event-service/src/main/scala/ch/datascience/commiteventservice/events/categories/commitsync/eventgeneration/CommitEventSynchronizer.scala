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

import cats.MonadThrow
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal.{CommitInfoFinder, EventDetailsFinder}
import ch.datascience.commiteventservice.events.categories.commitsync.{CommitProject, CommitSyncEvent, FullCommitSyncEvent, MinimalCommitSyncEvent, categoryName, logMessageCommon}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.events.CommitId
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
    missedEventsGenerator: MissedEventsGenerator[Interpretation],
    commitEventsRemover:   CommitEventsRemover[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation]
) extends CommitEventSynchronizer[Interpretation] {

  import executionTimeRecorder._
  override def synchronizeEvents(event: CommitSyncEvent): Interpretation[Unit] = (for {
    maybeAccessToken  <- accessTokenFinder.findAccessToken(event.project.id)
    maybeLatestCommit <- latestCommitFinder.findLatestCommit(event.project.id, maybeAccessToken).value
    _                 <- checkForSkippedEvent(maybeLatestCommit, event, maybeAccessToken)
  } yield ()) recoverWith loggingError(event)

  private def checkForSkippedEvent(maybeLatestCommit: Option[CommitInfo],
                                   event:             CommitSyncEvent,
                                   maybeAccessToken:  Option[AccessToken]
  ) = (maybeLatestCommit, event) match {
    case (Some(commitInfo), FullCommitSyncEvent(id, _, _)) if commitInfo.id == id =>
      measureExecutionTime(Skipped.pure[Interpretation].widen[UpdateResult]) >>= logResult(event)
    case (Some(commitInfo), event)             => processCommits(List(commitInfo.id), event.project, maybeAccessToken)
    case (None, FullCommitSyncEvent(id, _, _)) => processCommits(List(id), event.project, maybeAccessToken)
    case (None, MinimalCommitSyncEvent(_)) =>
      measureExecutionTime(Skipped.pure[Interpretation].widen[UpdateResult]) >>= logResult(event)
  }

  private def processCommits(commitList: List[CommitId],
                             project:    CommitProject,
                             maybeToken: Option[AccessToken]
  ): Interpretation[Unit] =
    commitList match {
      case commitId :: commitIds =>
        measureExecutionTime(
          getInfoFromELandGL(commitId, project, maybeToken)
            .flatMap {
              case (None, Some(commitFromGL)) =>
                missedEventsGenerator
                  .generateMissedEvents(project, commitFromGL.id)
                  .map(result => (result, commitId, commitIds ++: commitFromGL.parents))
              case (Some(commitFromEL), None) =>
                commitEventsRemover
                  .removeDeletedEvent(project, commitFromEL.id)
                  .map(result => (result, commitId, commitIds ++: commitFromEL.parents))
              case _ => Skipped.pure[Interpretation].widen[UpdateResult].map(result => (result, commitId, commitIds))
            }
            .recoverWith { case NonFatal(error) =>
              Failed(s"Synchronization failed", error)
                .pure[Interpretation]
                .widen[UpdateResult]
                .map(result => (result, commitId, commitIds))
            }
        )
          .flatMap { case (elapsedTime, (result, commitId, commits)) =>
            logResult(commitId, project)(elapsedTime -> result).flatMap(_ =>
              processCommits(commits, project, maybeToken)
            )
          }
      case Nil => ().pure[Interpretation]
    }

  private def getInfoFromELandGL(commitId:         CommitId,
                                 project:          CommitProject,
                                 maybeAccessToken: Option[AccessToken]
  ): Interpretation[(Option[CommitWithParents], Option[CommitInfo])] =
    for {
      maybeEventDetailsFromEL <- eventDetailsFinder.getEventDetails(project.id, commitId)
      maybeInfoFromGL         <- commitInfoFinder.getMaybeCommitInfo(project.id, commitId, maybeAccessToken)
    } yield (maybeEventDetailsFromEL, maybeInfoFromGL)

  private def loggingError(event: CommitSyncEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger
        .error(exception)(s"${logMessageCommon(event)} -> Synchronization failed")
        .flatMap(_ => exception.raiseError[Interpretation, Unit])

  }

  private def logResult(event: CommitSyncEvent): ((ElapsedTime, UpdateResult)) => Interpretation[Unit] = {
    case (elapsedTime, Skipped) =>
      logger.info(s"${logMessageCommon(event)} -> no new events found in ${elapsedTime}ms")
    case (elapsedTime, Updated) =>
      logger.info(s"${logMessageCommon(event)} -> new events found in ${elapsedTime}ms")
    case (elapsedTime, Deleted) =>
      logger.info(s"${logMessageCommon(event)} -> events found for deletion in ${elapsedTime}ms")
    case (elapsedTime, Failed(message, exception)) =>
      logger.error(exception)(s"${logMessageCommon(event)} -> $message in ${elapsedTime}ms")
  }

  private def logResult(eventId: CommitId,
                        project: CommitProject
  ): ((ElapsedTime, UpdateResult)) => Interpretation[Unit] = {
    case (elapsedTime, Skipped) =>
      logger.info(
        s"$categoryName: id = $eventId, projectId = ${project.id}, projectPath = ${project.path} -> no new events found in ${elapsedTime}ms"
      )
    case (elapsedTime, Updated) =>
      logger.info(
        s"$categoryName: id = $eventId, projectId = ${project.id}, projectPath = ${project.path} -> new events found in ${elapsedTime}ms"
      )
    case (elapsedTime, Deleted) =>
      logger.info(
        s"$categoryName: id = $eventId, projectId = ${project.id}, projectPath = ${project.path} -> events found for deletion in ${elapsedTime}ms"
      )
    case (elapsedTime, Failed(message, exception)) =>
      logger.error(exception)(
        s"$categoryName: id = $eventId, projectId = ${project.id}, projectPath = ${project.path} -> $message in ${elapsedTime}ms"
      )
  }

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
    accessTokenFinder     <- AccessTokenFinder(logger)
    latestCommitFinder    <- LatestCommitFinder(gitLabThrottler, logger)
    eventDetailsFinder    <- EventDetailsFinder(logger)
    commitInfoFinder      <- CommitInfoFinder(gitLabThrottler, logger)
    missedEventsGenerator <- MissedEventsGenerator(gitLabThrottler, executionTimeRecorder, logger)
    commitEventRemover    <- CommitEventsRemover(logger)

  } yield new CommitEventSynchronizerImpl[IO](accessTokenFinder,
                                              latestCommitFinder,
                                              eventDetailsFinder,
                                              commitInfoFinder,
                                              missedEventsGenerator,
                                              commitEventRemover,
                                              executionTimeRecorder,
                                              logger
  )

  sealed trait UpdateResult extends Product with Serializable
  object UpdateResult {
    final case object Skipped extends UpdateResult
    final case object Updated extends UpdateResult
    final case object Deleted extends UpdateResult
    case class Failed(message: String, exception: Throwable) extends UpdateResult
  }
}
