/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.triplesgenerator.events.consumers.minprojectinfo

import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel, Parallel}
import io.renku.graph.model.{RenkuUrl, datasets}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.lock.Lock
import io.renku.logging.ExecutionTimeRecorder
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.metrics.{Histogram, MetricsRegistry}
import io.renku.tokenrepository.api.TokenRepositoryClient
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.ProjectActivated
import io.renku.triplesgenerator.errors.ProcessingRecoverableError.{LogWorthyRecoverableError, SilentRecoverableError}
import io.renku.triplesgenerator.errors.{ProcessingNonRecoverableError, ProcessingRecoverableError}
import io.renku.triplesgenerator.tsprovisioning.triplesuploading.TriplesUploadResult
import io.renku.triplesgenerator.tsprovisioning.triplesuploading.TriplesUploadResult.{DeliverySuccess, NonRecoverableFailure, RecoverableFailure}
import io.renku.triplesgenerator.tsprovisioning.{TSProvisioner, UploadingResult}
import io.renku.triplesstore.{ProjectSparqlClient, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait EventProcessor[F[_]] {
  def process(event: MinProjectInfoEvent): F[Unit]
}

private class EventProcessorImpl[F[_]: MonadThrow: Logger](
    trClient:                TokenRepositoryClient[F],
    tsProvisioner:           TSProvisioner[F],
    entityBuilder:           EntityBuilder[F],
    projectExistenceChecker: ProjectExistenceChecker[F],
    tgClient:                triplesgenerator.api.events.Client[F],
    executionTimeRecorder:   ExecutionTimeRecorder[F]
) extends EventProcessor[F] {

  import EventUploadingResult._
  import entityBuilder._
  import executionTimeRecorder._
  import projectExistenceChecker._
  import trClient._

  override def process(event: MinProjectInfoEvent): F[Unit] = {
    Logger[F].info(s"${prefix(event)} accepted") >>
      measureExecutionTime {
        findAccessToken(event.project.slug).flatMap {
          case Some(implicit0(at: AccessToken)) => transformAndUpload(event)
          case None =>
            val err = SilentRecoverableError("No access token")
            logError(event)(maybeMessage = err.getMessage.some).as(RecoverableError(event, err).widen)
        }
      }.flatTap(logSummary(event))
        .flatMap(sendProjectActivatedEvent(event))
  } recoverWith errorLogging(event)

  private def errorLogging(event: MinProjectInfoEvent): PartialFunction[Throwable, F[Unit]] = { case exception =>
    logError(event)(maybeCause = exception.some)
  }

  private def logError(
      event: MinProjectInfoEvent
  )(maybeMessage: Option[String] = None, maybeCause: Option[Throwable] = None): F[Unit] =
    (maybeMessage map (m => Logger[F].error(show"$categoryName: $event processing failure -> $m")))
      .orElse(maybeCause map (Logger[F].error(_)(show"$categoryName: $event processing failure")))
      .getOrElse(Logger[F].error(show"$categoryName: $event processing failure"))

  private def transformAndUpload(
      event: MinProjectInfoEvent
  )(implicit at: AccessToken): F[EventUploadingResult] =
    (buildEntity(event) leftSemiflatMap toUploadingError(event)).semiflatMap { project =>
      checkProjectExists(project.resourceId) >>= {
        case true  => Skipped(event).widen.pure[F]
        case false => tsProvisioner.provisionTS(project) >>= toUploadingResult(event)
      }
    }.merge recoverWith nonRecoverableFailure(event)

  private def toUploadingError(event: MinProjectInfoEvent): PartialFunction[Throwable, F[EventUploadingResult]] = {
    case error: LogWorthyRecoverableError =>
      Logger[F]
        .error(error)(s"${prefix(event)} ${error.getMessage}")
        .map(_ => RecoverableError(event, error))
    case error: SilentRecoverableError =>
      RecoverableError(event, error).pure[F].widen[EventUploadingResult]
  }

  private def toUploadingResult(event: MinProjectInfoEvent): TriplesUploadResult => F[EventUploadingResult] = {
    case DeliverySuccess => Uploaded(event).pure[F].widen
    case RecoverableFailure(error @ LogWorthyRecoverableError(message, _)) =>
      Logger[F]
        .error(error)(s"${prefix(event)} $message")
        .map(_ => RecoverableError(event, error))
    case RecoverableFailure(error @ SilentRecoverableError(_, _)) =>
      RecoverableError(event, error).pure[F].widen[EventUploadingResult]
    case error: NonRecoverableFailure =>
      Logger[F]
        .error(error)(s"${prefix(event)} ${error.message}")
        .map(_ => NonRecoverableError(event, error: Throwable))
  }

  private def nonRecoverableFailure(event: MinProjectInfoEvent): PartialFunction[Throwable, F[EventUploadingResult]] = {
    case exception: ProcessingNonRecoverableError.MalformedRepository =>
      NonRecoverableError(event, exception).pure[F].widen[EventUploadingResult]
    case NonFatal(exception) =>
      Logger[F]
        .error(exception)(s"${prefix(event)} ${exception.getMessage}")
        .map(_ => NonRecoverableError(event, exception))
  }

  private def sendProjectActivatedEvent(
      event: MinProjectInfoEvent
  ): ((ElapsedTime, EventUploadingResult)) => F[Unit] = {
    case (_, Uploaded(_)) =>
      tgClient
        .send(ProjectActivated.forProject(event.project.slug))
        .handleErrorWith(Logger[F].error(_)(s"${prefix(event)} sending ${ProjectActivated.categoryName} event failed"))
    case _ => ().pure[F]
  }

  private def logSummary(event: MinProjectInfoEvent): ((ElapsedTime, EventUploadingResult)) => F[Unit] = {
    case (elapsedTime, uploadingResult) =>
      val message = uploadingResult match {
        case Uploaded(_) => "success"
        case Skipped(_)  => "skipped"
        case _           => "failure"
      }
      Logger[F].info(s"${prefix(event)} processed in ${elapsedTime}ms: $message")
  }

  private def prefix(event: MinProjectInfoEvent): String = show"$categoryName: $event"

  private sealed trait EventUploadingResult extends UploadingResult with Product {
    val event: MinProjectInfoEvent
    val widen: EventUploadingResult = this
  }
  private sealed trait ProcessingSuccessful extends EventUploadingResult
  private sealed trait UploadingError extends EventUploadingResult {
    val cause: Throwable
  }

  private object EventUploadingResult {
    case class Skipped(event: MinProjectInfoEvent)  extends ProcessingSuccessful with UploadingResult.Uploaded
    case class Uploaded(event: MinProjectInfoEvent) extends ProcessingSuccessful with UploadingResult.Uploaded

    case class RecoverableError(event: MinProjectInfoEvent, cause: ProcessingRecoverableError)
        extends UploadingError
        with UploadingResult.RecoverableError

    case class NonRecoverableError(event: MinProjectInfoEvent, cause: Throwable)
        extends UploadingError
        with UploadingResult.NonRecoverableError
  }
}

private object EventProcessor {

  import eu.timepit.refined.auto._

  def apply[F[_]: Async: NonEmptyParallel: Parallel: GitLabClient: Logger: MetricsRegistry: SparqlQueryTimeRecorder](
      topSameAsLock:       Lock[F, datasets.TopmostSameAs],
      projectSparqlClient: ProjectSparqlClient[F]
  )(implicit renkuUrl: RenkuUrl): F[EventProcessor[F]] = for {
    trClient                <- TokenRepositoryClient[F]
    tsProvisioner           <- TSProvisioner[F](topSameAsLock, projectSparqlClient)
    entityBuilder           <- EntityBuilder[F]
    projectExistenceChecker <- ProjectExistenceChecker[F]
    tgClient                <- triplesgenerator.api.events.Client[F]
    eventsProcessingTimes <- Histogram(
                               name = "min_project_info_processing_times",
                               help = "Min project info processing times",
                               buckets = Seq(.1, .5, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000,
                                             1000000, 5000000, 10000000, 50000000, 100000000, 500000000)
                             )
    executionTimeRecorder <- ExecutionTimeRecorder[F](maybeHistogram = Some(eventsProcessingTimes))
  } yield new EventProcessorImpl(trClient,
                                 tsProvisioner,
                                 entityBuilder,
                                 projectExistenceChecker,
                                 tgClient,
                                 executionTimeRecorder
  )
}
