package ch.datascience.commiteventservice.events.categories.commitsync
package eventgeneration

import cats.MonadThrow
import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.EventStatusPatcher
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult._
import ch.datascience.graph.model.events.CommitId
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[commitsync] trait CommitEventsRemover[Interpretation[_]] {
  def removeDeletedEvent(commitId: CommitId, project: CommitProject): Interpretation[UpdateResult]
}

private class CommitEventsRemoverImpl[Interpretation[_]: MonadThrow](
    eventStatusPatcher: EventStatusPatcher[Interpretation]
) extends CommitEventsRemover[Interpretation] {
  override def removeDeletedEvent(commitId: CommitId, project: CommitProject): Interpretation[UpdateResult] =
    eventStatusPatcher
      .sendDeletionStatus(commitId, project.id)
      .map(_ => Deleted: UpdateResult) recoverWith { case NonFatal(e) =>
      Failed(s"$categoryName - Commit Remover failed to send commit deletion status", e)
        .pure[Interpretation]
        .widen[UpdateResult]
    }

}

private[commitsync] object CommitEventsRemover {

  def apply(logger:     Logger[IO])(implicit
      concurrentEffect: ConcurrentEffect[IO],
      timer:            Timer[IO],
      executionContext: ExecutionContext
  ): IO[CommitEventsRemover[IO]] = for {
    eventStatusPatcher <- EventStatusPatcher(logger)
  } yield new CommitEventsRemoverImpl[IO](eventStatusPatcher)
}
