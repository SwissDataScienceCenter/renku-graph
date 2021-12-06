package io.renku.triplesgenerator.events.categories.cleanup

import cats.effect.Async
import cats.syntax.all._
import io.renku.events.consumers.Project
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait EventProcessor[F[_]] {
  def process(project: Project): F[Unit]
}

private class CleanUpEventProcessorImpl[F[_]: Async: Logger](triplesRemover: ProjectTriplesRemover[F],
                                                             eventLogNotifier: EventLogNotifier[F]
) extends EventProcessor[F] {
  override def process(project: Project): F[Unit] = for {
    _ <- triplesRemover.removeTriples(of = project.path) recoverWith logErrorAndThrow(project, " failed")
    _ <-
      eventLogNotifier.notifyEventLog(project) recoverWith logErrorAndThrow(project, ", event log notification failed")
  } yield ()

  private def logErrorAndThrow(project: Project, message: String): PartialFunction[Throwable, F[Unit]] = {
    case NonFatal(error) =>
      Logger[F].error(error)(s"${commonLogMessage(project)} - Triples removal$message ${error.getMessage}") >> error
        .raiseError[F, Unit]
  }

  private def commonLogMessage(project: Project): String =
    s"$categoryName: ${project.show}"
}

private object CleanUpEventProcessor {
  def apply[F[_]: Async: Logger](sparqlQueryTimeRecorder: SparqlQueryTimeRecorder[F]): F[EventProcessor[F]] =
    for {
      eventLogNotifier <- EventLogNotifier[F]
      triplesRemover   <- ProjectTriplesRemover(sparqlQueryTimeRecorder)
    } yield new CleanUpEventProcessorImpl[F](triplesRemover, eventLogNotifier)
}
