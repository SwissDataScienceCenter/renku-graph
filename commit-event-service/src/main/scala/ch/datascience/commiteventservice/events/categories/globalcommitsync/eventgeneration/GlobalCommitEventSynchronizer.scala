package ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration

import cats.MonadThrow
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.EventStatusPatcher
import ch.datascience.commiteventservice.events.categories.globalcommitsync.GlobalCommitSyncEvent
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.graph.tokenrepository.AccessTokenFinder._
import ch.datascience.logging.ExecutionTimeRecorder
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[globalcommitsync] trait GlobalCommitEventSynchronizer[Interpretation[_]] {
  def synchronizeEvents(event: GlobalCommitSyncEvent): Interpretation[Unit]
}
private[globalcommitsync] class GlobalCommitEventSynchronizerImpl[Interpretation[_]: MonadThrow](
    accessTokenFinder:     AccessTokenFinder[Interpretation],
    gitLabCommitFetcher:   GitLabCommitFetcher[Interpretation],
    eventStatusPatcher:    EventStatusPatcher[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation],
    clock:                 java.time.Clock = java.time.Clock.systemUTC()
) extends GlobalCommitEventSynchronizer[Interpretation] {

  import accessTokenFinder._
  import gitLabCommitFetcher._

  override def synchronizeEvents(event: GlobalCommitSyncEvent): Interpretation[Unit] = for {
    maybeAccessToken <- findAccessToken(event.project.id)
    commitStats      <- fetchCommitStats(event.project.id)(maybeAccessToken)

    allGitLabCommits <- fetchAllGitLabCommits(event.project.id)(maybeAccessToken)
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
    eventStatusPatcher  <- EventStatusPatcher(logger)
  } yield new GlobalCommitEventSynchronizerImpl(
    accessTokenFinder,
    gitLabCommitFetcher,
    eventStatusPatcher,
    executionTimeRecorder,
    logger
  )
}
