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

import cats.data.StateT
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import cats.{Applicative, MonadThrow}
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.SynchronizationSummary.{SummaryState, add}
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.{SynchronizationSummary, UpdateResult}
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal.{CommitInfoFinder, EventDetailsFinder}
import ch.datascience.commiteventservice.events.categories.commitsync._
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
    case (Some(commitInfo), event) =>
      measureExecutionTime(
        processCommits(List(commitInfo.id), event.project, maybeAccessToken).run(SynchronizationSummary())
      ) flatMap { case (elapsedTime: ElapsedTime, summary) =>
        logSummary(commitInfo.id, event.project)(elapsedTime, summary._2)
      }
    case (None, FullCommitSyncEvent(id, _, _)) =>
      measureExecutionTime(
        processCommits(List(id), event.project, maybeAccessToken).run(SynchronizationSummary())
      ) flatMap { case (elapsedTime: ElapsedTime, summary) =>
        logSummary(id, event.project)(elapsedTime, summary._2)
      }
    case (None, MinimalCommitSyncEvent(_)) =>
      measureExecutionTime(Skipped.pure[Interpretation].widen[UpdateResult]) >>= logResult(event)
  }

  private def processCommits(commitList: List[CommitId],
                             project:    CommitProject,
                             maybeToken: Option[AccessToken]
  ): SummaryState[Interpretation, SynchronizationSummary] =
    commitList match {
      case commitId :: commitIds =>
        StateT
          .liftF(
            measureExecutionTime(
              getInfoFromELandGL(commitId, project, maybeToken)
                .flatMap {
                  case (None, Some(commitFromGL)) =>
                    missedEventsGenerator
                      .generateMissedEvents(project, commitFromGL.id, maybeToken)
                      .map(result => (result, commitId, commitIds ++: commitFromGL.parents))
                  case (Some(commitFromEL), None) =>
                    commitEventsRemover
                      .removeDeletedEvent(project, commitFromEL.id)
                      .map(result => (result, commitId, commitIds ++: commitFromEL.parents))
                  case _ =>
                    Skipped.pure[Interpretation].widen[UpdateResult].map(result => (result, commitId, commitIds))
                }
                .recoverWith { case NonFatal(error) =>
                  Failed(s"Synchronization failed", error)
                    .pure[Interpretation]
                    .widen[UpdateResult]
                    .map(result => (result, commitId, commitIds))
                }
            )
          )
          .flatMap { case (elapsedTime, (result, commitId, commits)) =>
            for {
              _               <- add[Interpretation](result)
              _               <- StateT.liftF(logResult(commitId, project)(elapsedTime -> result))
              continueProcess <- processCommits(commits, project, maybeToken)
            } yield continueProcess
          }
      case Nil => StateT.get[Interpretation, SynchronizationSummary]
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
    case (elapsedTime, Created | Existed) =>
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
      logger.info(logMessageFor(eventId, project, s"no new events found in ${elapsedTime}ms"))
    case (elapsedTime, Created | Existed) =>
      logger.info(logMessageFor(eventId, project, s"new events found in ${elapsedTime}ms"))
    case (elapsedTime, Deleted) =>
      logger.info(
        logMessageFor(eventId, project, s"events found for deletion in ${elapsedTime}ms")
      )
    case (elapsedTime, Failed(message, exception)) =>
      logger.error(exception)(logMessageFor(eventId, project, s"$message in ${elapsedTime}ms"))
  }

  private def logSummary(eventId: CommitId,
                         project: CommitProject
  ): ((ElapsedTime, SynchronizationSummary)) => Interpretation[Unit] = { case (elapsedTime, summary) =>
    logger.info(
      logMessageFor(
        eventId,
        project,
        s"events generation result: ${summary.created} created, ${summary.existed} existed, ${summary.deleted} deleted, ${summary.failed} failed in ${elapsedTime}ms"
      )
    )

  }

  private def logMessageFor(eventId: CommitId, project: CommitProject, message: String) =
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
    accessTokenFinder     <- AccessTokenFinder(logger)
    latestCommitFinder    <- LatestCommitFinder(gitLabThrottler, logger)
    eventDetailsFinder    <- EventDetailsFinder(logger)
    commitInfoFinder      <- CommitInfoFinder(gitLabThrottler, logger)
    missedEventsGenerator <- MissedEventsGenerator(gitLabThrottler, logger)
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
    final case object Created extends UpdateResult
    final case object Existed extends UpdateResult
    final case object Deleted extends UpdateResult
    case class Failed(message: String, exception: Throwable) extends UpdateResult
  }

  final case class SynchronizationSummary(skipped: Int = 0,
                                          created: Int = 0,
                                          existed: Int = 0,
                                          deleted: Int = 0,
                                          failed:  Int = 0
  )

  object SynchronizationSummary {
    type SummaryState[F[_], A] = StateT[F, SynchronizationSummary, A]

    def add[F[_]: Applicative](result: UpdateResult): SummaryState[F, Unit] = StateT {
      case SynchronizationSummary(s, c, e, d, f) =>
        result match {
          case Skipped      => (SynchronizationSummary(s + 1, c, e, d, f), ()).pure[F]
          case Created      => (SynchronizationSummary(s, c + 1, e, d, f), ()).pure[F]
          case Existed      => (SynchronizationSummary(s, c, e + 1, d, f), ()).pure[F]
          case Deleted      => (SynchronizationSummary(s, c, e, d + 1, f), ()).pure[F]
          case Failed(_, _) => (SynchronizationSummary(s, c, e, d, f + 1), ()).pure[F]
        }

    }
  }

}
