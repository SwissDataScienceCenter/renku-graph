package ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.commiteventservice.events.EventStatusPatcher
import ch.datascience.commiteventservice.events.categories.globalcommitsync.GlobalCommitSyncEvent
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.history.EventDetailsFinder
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.logging.ExecutionTimeRecorder
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[globalcommitsync] trait GlobalCommitEventSynchronizer[Interpretation[_]] {
  def synchronizeEvents(event: GlobalCommitSyncEvent): Interpretation[Unit]
}
private[globalcommitsync] class GlobalCommitEventSynchronizerImpl[Interpretation[_]](
    accessTokenFinder:     AccessTokenFinder[Interpretation],
    eventDetailsFinder:    EventDetailsFinder[Interpretation],
    eventStatusPatcher:    EventStatusPatcher[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation]
) extends GlobalCommitEventSynchronizer[Interpretation] {
  override def synchronizeEvents(event: GlobalCommitSyncEvent): Interpretation[Unit] = ???
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
    accessTokenFinder  <- AccessTokenFinder(logger)
    eventDetailsFinder <- EventDetailsFinder(logger)
    eventStatusPatcher <- EventStatusPatcher(logger)
  } yield new GlobalCommitEventSynchronizerImpl(
    accessTokenFinder,
    eventDetailsFinder,
    eventStatusPatcher,
    executionTimeRecorder,
    logger
  )
}
