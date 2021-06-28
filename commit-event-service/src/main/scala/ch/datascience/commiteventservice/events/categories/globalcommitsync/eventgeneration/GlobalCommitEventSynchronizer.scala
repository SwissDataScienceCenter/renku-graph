package ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration

import cats.MonadThrow
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.commiteventservice.events.EventStatusPatcher
import ch.datascience.commiteventservice.events.categories.globalcommitsync.GlobalCommitSyncEvent
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.history.EventDetailsFinder
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.logging.ExecutionTimeRecorder
import org.typelevel.log4cats.Logger
import ch.datascience.graph.tokenrepository.AccessTokenFinder._
import cats.syntax.all._

import scala.concurrent.ExecutionContext

private[globalcommitsync] trait GlobalCommitEventSynchronizer[Interpretation[_]] {
  def synchronizeEvents(event: GlobalCommitSyncEvent): Interpretation[Unit]
}
private[globalcommitsync] class GlobalCommitEventSynchronizerImpl[Interpretation[_]: MonadThrow](
    accessTokenFinder:     AccessTokenFinder[Interpretation],
    gitLabCommitFetcher:   GitLabCommitFetcher[Interpretation],
    eventDetailsFinder:    EventDetailsFinder[Interpretation],
    eventStatusPatcher:    EventStatusPatcher[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation],
    clock:                 java.time.Clock = java.time.Clock.systemUTC()
) extends GlobalCommitEventSynchronizer[Interpretation] {

  import accessTokenFinder._
  import gitLabCommitFetcher._
  import eventDetailsFinder._
  import eventStatusPatcher._
  import executionTimeRecorder._

  override def synchronizeEvents(event: GlobalCommitSyncEvent): Interpretation[Unit] = for {
    maybeAccessToken <- findAccessToken(event.project.id)
    allGitLabCommits <- fetchAllGitLabCommits(event.project.id, maybeAccessToken)
  } yield ()
}

private[globalcommitsync] object GlobalCommitEventSynchronizer {
  def apply(gitLabThrottler:       Throttler[IO, GitLab],
            executionTimeRecorder: ExecutionTimeRecorder[IO],
            logger:                Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[GlobalCommitEventSynchronizer[IO]] = for {
    accessTokenFinder   <- AccessTokenFinder(logger)
    gitLabCommitFetcher <- GitLabCommitFetcher(gitLabThrottler, logger)
    eventDetailsFinder  <- EventDetailsFinder(logger)
    eventStatusPatcher  <- EventStatusPatcher(logger)
  } yield new GlobalCommitEventSynchronizerImpl(
    accessTokenFinder,
    gitLabCommitFetcher,
    eventDetailsFinder,
    eventStatusPatcher,
    executionTimeRecorder,
    logger
  )
}
