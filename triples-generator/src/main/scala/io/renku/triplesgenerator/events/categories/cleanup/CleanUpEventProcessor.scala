package io.renku.triplesgenerator.events.categories.cleanup

import cats.syntax.all._
import cats.effect.MonadCancelThrow
import io.renku.events.consumers.Project
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

private trait EventProcessor[F[_]] {
  def process(project: Project): F[Unit]
}

private class CleanUpEventProcessorImpl[F[_]: Logger]() extends EventProcessor[F] {
  override def process(project: Project): F[Unit] = ???
}

private object CleanUpEventProcessor {
  def apply[F[_]: MonadCancelThrow: Logger](sparqlQueryTimeRecorder: SparqlQueryTimeRecorder[F]): F[EventProcessor[F]] =
    for {
      _ <- println(sparqlQueryTimeRecorder.toString).pure[F]
    } yield new CleanUpEventProcessorImpl[F]()
}
