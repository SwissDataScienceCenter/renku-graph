package io.renku.eventlog.jobs

import cats.MonadError
import cats.data.EitherT
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.graph.model.CliVersion
import ch.datascience.http.client.IORestClient
import ch.datascience.http.client.IORestClient.SleepAfterConnectionIssue
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.Event
import io.renku.eventlog.jobs.K8sJobCreator.SendingResult

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.implicits._

trait K8sJobCreator[Interpretation[_], CategoryEvent] {
  def createJob(cliVersion: CliVersion, event: CategoryEvent): Interpretation[SendingResult]
}

private class K8sJobCreatorImpl[CategoryEvent](logger:        Logger[IO],
                                               retryInterval: FiniteDuration = SleepAfterConnectionIssue
)(implicit
    ME:               MonadError[IO, Throwable],
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]
) extends IORestClient(Throttler.noThrottling, logger, retryInterval = retryInterval)
    with K8sJobCreator[IO, CategoryEvent] {
  override def createJob(cliVersion: CliVersion, event: CategoryEvent): IO[SendingResult] = ???
}

private object K8sJobCreator {
  sealed trait SendingResult extends Product with Serializable
  object SendingResult {
    case object Delivered    extends SendingResult
    case object ServiceBusy  extends SendingResult
    case object Misdelivered extends SendingResult
  }

  val resourceUri: Uri = uri"/apis" / "batch" / "v1" / "jobs"

}
