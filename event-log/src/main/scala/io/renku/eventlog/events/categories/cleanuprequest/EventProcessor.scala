package io.renku.eventlog.events.categories.cleanuprequest

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.metrics.LabeledHistogram

private trait EventProcessor[F[_]] {
  def process(event: CleanUpRequestEvent): F[Unit]
}

private object EventProcessor {
  def apply[F[_]: Async: SessionResource](queriesExecTimes: LabeledHistogram[F]): F[EventProcessor[F]] = for {
    projectIdFinder <- ProjectIdFinder[F](queriesExecTimes)
    queue           <- CleanUpEventsQueue[F](queriesExecTimes)
  } yield new EventProcessorImpl[F](projectIdFinder, queue)
}

private class EventProcessorImpl[F[_]: MonadThrow](projectIdFinder: ProjectIdFinder[F], queue: CleanUpEventsQueue[F])
    extends EventProcessor[F] {

  import projectIdFinder._

  override def process(event: CleanUpRequestEvent): F[Unit] = event match {
    case CleanUpRequestEvent.Full(id, path) => queue.offer(id, path)
    case CleanUpRequestEvent.Partial(path) =>
      findProjectId(path) >>= {
        case Some(id) => queue.offer(id, path)
        case None     => new Exception(show"Cannot find projectId for $path").raiseError
      }
  }
}
