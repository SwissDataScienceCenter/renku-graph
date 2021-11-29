package io.renku.triplesgenerator.events.categories.cleanup

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.events.consumers.Project
import io.renku.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import io.renku.rdfstore.{RdfStoreClientImpl, RdfStoreConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import scala.language.postfixOps

private trait ProjectTriplesRemover[F[_]] {
  def removeTriples(of: Project): F[Unit]
}

private object ProjectTriplesRemover {
  def apply[F[_]: Async: Logger](rdfStoreConfig: RdfStoreConfig,
                                 timeRecorder:   SparqlQueryTimeRecorder[F],
                                 retryInterval:  FiniteDuration = SleepAfterConnectionIssue,
                                 maxRetries:     Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
                                 idleTimeout:    Duration = 6 minutes,
                                 requestTimeout: Duration = 5 minutes
  ): F[ProjectTriplesRemover[F]] =
    MonadThrow[F].catchNonFatal(
      new ProjectTriplesRemoverImpl[F](rdfStoreConfig,
                                       timeRecorder,
                                       retryInterval,
                                       maxRetries,
                                       idleTimeout,
                                       requestTimeout
      )
    )
}
private class ProjectTriplesRemoverImpl[F[_]: Async: Logger](
    rdfStoreConfig: RdfStoreConfig,
    timeRecorder:   SparqlQueryTimeRecorder[F],
    retryInterval:  FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:     Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeout:    Duration = 6 minutes,
    requestTimeout: Duration = 5 minutes
) extends RdfStoreClientImpl(rdfStoreConfig,
                             timeRecorder,
                             retryInterval = retryInterval,
                             maxRetries = maxRetries,
                             idleTimeoutOverride = idleTimeout.some,
                             requestTimeoutOverride = requestTimeout.some
    )
    with ProjectTriplesRemover[F] {
  override def removeTriples(of: Project): F[Unit] = ???
}
