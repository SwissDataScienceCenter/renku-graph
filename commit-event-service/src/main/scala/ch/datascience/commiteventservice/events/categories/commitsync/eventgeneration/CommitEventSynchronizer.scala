package ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration

import cats.MonadThrow
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal.{CommitInfoFinder, EventDetailsFinder}
import ch.datascience.commiteventservice.events.categories.commitsync.{CommitProject, CommitSyncEvent, FullCommitSyncEvent, MinimalCommitSyncEvent, logMessageCommon}
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
  override def synchronizeEvents(event: CommitSyncEvent): Interpretation[Unit] = for {
    maybeAccessToken <- accessTokenFinder.findAccessToken(event.project.id)
    maybeLatestCommit <-
      latestCommitFinder
        .findLatestCommit(event.project.id, maybeAccessToken)
        .value

    result <- measureExecutionTime(checkForSkippedEvent(maybeLatestCommit, event, maybeAccessToken)) flatMap logResult(
                event
              ) recoverWith loggingError(event)
  } yield result

  private def checkForSkippedEvent(maybeLatestCommit: Option[CommitInfo],
                                   event:             CommitSyncEvent,
                                   maybeAccessToken:  Option[AccessToken]
  ) = (maybeLatestCommit, event) match {
    case (Some(commitInfo), FullCommitSyncEvent(id, _, _)) if commitInfo.id == id =>
      Skipped.pure[Interpretation].widen[UpdateResult]
    case (Some(commitInfo), event)             => processCommits(commitInfo.id, event.project, maybeAccessToken)
    case (None, FullCommitSyncEvent(id, _, _)) => processCommits(id, event.project, maybeAccessToken)
    case (None, MinimalCommitSyncEvent(_))     => Skipped.pure[Interpretation].widen[UpdateResult]
  }

  private def processCommits(commitId:   CommitId,
                             project:    CommitProject,
                             maybeToken: Option[AccessToken]
  ): Interpretation[UpdateResult] =
    getInfoFromELandGL(commitId, project, maybeToken).flatMap {
      case (None, Some(commitFromGL)) =>
        (commitFromGL.id +: commitFromGL.parents).foldLeftM[Interpretation, UpdateResult](Skipped) {
          case (_, commitId) =>
            missedEventsGenerator
              .generateMissedEvents(commitId, project)
              .flatMap(_ => processCommits(commitId, project, maybeToken))
        }
      case (Some(commitFromEL), None) =>
        (commitFromEL.id +: commitFromEL.parents).foldLeftM[Interpretation, UpdateResult](Skipped) {
          case (_, commitId) =>
            commitEventsRemover
              .removeDeletedEvent(commitId, project)
              .flatMap(_ => processCommits(commitId, project, maybeToken))
        }
      case _ => Skipped.pure[Interpretation].widen[UpdateResult]
    }

  private def getInfoFromELandGL(commitId:         CommitId,
                                 project:          CommitProject,
                                 maybeAccessToken: Option[AccessToken]
  ): Interpretation[(Option[CommitInfo], Option[CommitInfo])] =
    for {
      existsInEL      <- eventDetailsFinder.getEventDetails(commitId, project.id)
      maybeInfoFromGL <- commitInfoFinder.getMaybeCommitInfo(project.id, commitId, maybeAccessToken)
    } yield (existsInEL, maybeInfoFromGL)

  private def loggingError(event: CommitSyncEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"${logMessageCommon(event)} -> synchronization failed")
      exception.raiseError[Interpretation, Unit]
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
