package ch.datascience.commiteventservice.commitsync
import cats.MonadError
import cats.effect.{Clock, ContextShift, Effect, IO, Timer}
import ch.datascience.graph.model.projects.Id
import ch.datascience.logging.ExecutionTimeRecorder
import io.chrisdavenport.log4cats.Logger
import org.http4s.{Request, Response}
import org.http4s.dsl.Http4sDsl

import scala.concurrent.ExecutionContext

trait CommitSyncEndpoint[Interpretation[_]] {

  def syncCommits(request: Request[Interpretation]): Interpretation[Response[Interpretation]]
}
private class CommitSyncEndpointImpl[Interpretation[_]: Effect](
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    logger:                Logger[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation]
    with CommitSyncEndpoint[Interpretation] {

  def syncCommits(request: Request[Interpretation]): Interpretation[Response[Interpretation]] = ???
}

object CommitSyncEndpoint {
  def apply(executionTimeRecorder: ExecutionTimeRecorder[IO], logger: Logger[IO])(implicit
      executionContext:            ExecutionContext,
      contextShift:                ContextShift[IO],
      clock:                       Clock[IO],
      timer:                       Timer[IO]
  ): IO[CommitSyncEndpoint[IO]] = ???
}
