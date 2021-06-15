package ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.commiteventservice.events.categories.globalcommitsync.GlobalCommitSyncEvent
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.logging.ExecutionTimeRecorder
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[globalcommitsync] trait GlobalCommitEventSynchronizer[Interpretation[_]] {
  def synchronizeEvents(event: GlobalCommitSyncEvent): Interpretation[Unit]
}
private[globalcommitsync] class GlobalCommitEventSynchronizerImpl {}

private[globalcommitsync] object CommitEventSynchronizer {
  def apply(gitLabThrottler:       Throttler[IO, GitLab],
            executionTimeRecorder: ExecutionTimeRecorder[IO],
            logger:                Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[GlobalCommitEventSynchronizer[IO]] = ???
}
