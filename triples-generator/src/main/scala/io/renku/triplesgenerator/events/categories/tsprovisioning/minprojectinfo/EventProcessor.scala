/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories
package tsprovisioning
package minprojectinfo

import ProcessingRecoverableError.{LogWorthyRecoverableError, SilentRecoverableError}
import cats.data.EitherT.right
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel, Parallel}
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.logging.ExecutionTimeRecorder
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.metrics.{Histogram, MetricsRegistry}
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger
import transformation.TransformationStepsCreator
import triplesuploading.TriplesUploadResult.{DeliverySuccess, NonRecoverableFailure, RecoverableFailure}
import triplesuploading.{TransformationStepsRunner, TriplesUploadResult}

import scala.util.control.NonFatal

private trait EventProcessor[F[_]] {
  def process(event: MinProjectInfoEvent): F[Unit]
}

private class EventProcessorImpl[F[_]: MonadThrow: Logger](accessTokenFinder: AccessTokenFinder[F],
                                                           stepsCreator:          TransformationStepsCreator[F],
                                                           uploader:              TransformationStepsRunner[F],
                                                           entityBuilder:         EntityBuilder[F],
                                                           executionTimeRecorder: ExecutionTimeRecorder[F]
) extends EventProcessor[F] {

  import AccessTokenFinder._
  import UploadingResult._
  import accessTokenFinder._
  import entityBuilder._
  import executionTimeRecorder._
  import stepsCreator._
  import uploader._

  override def process(event: MinProjectInfoEvent): F[Unit] = {
    findAccessToken(event.project.path) >>= { implicit accessToken =>
      measureExecutionTime(transformAndUpload(event)) >>= logSummary(event)
    }
  } recoverWith logError(event)

  private def logError(event: MinProjectInfoEvent): PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(show"$categoryName: processing failure: $event")
  }

  private def transformAndUpload(
      event:                   MinProjectInfoEvent
  )(implicit maybeAccessToken: Option[AccessToken]): F[UploadingResult] = {
    for {
      project <- buildEntity(event) leftSemiflatMap toUploadingError(event)
      result  <- right[UploadingResult](run(createSteps, project) >>= (toUploadingResult(event, _)))
    } yield result
  }.merge recoverWith nonRecoverableFailure(event)

  private def toUploadingError(event: MinProjectInfoEvent): PartialFunction[Throwable, F[UploadingResult]] = {
    case error: LogWorthyRecoverableError =>
      Logger[F]
        .error(error)(s"${logMessageCommon(event)} ${error.getMessage}")
        .map(_ => RecoverableError(event, error))
    case error: SilentRecoverableError =>
      RecoverableError(event, error).pure[F].widen[UploadingResult]
  }

  private def toUploadingResult(event:               MinProjectInfoEvent,
                                triplesUploadResult: TriplesUploadResult
  ): F[UploadingResult] = triplesUploadResult match {
    case DeliverySuccess => (Uploaded(event): UploadingResult).pure[F]
    case RecoverableFailure(error) =>
      error match {
        case error @ LogWorthyRecoverableError(message, _) =>
          Logger[F]
            .error(error)(s"${logMessageCommon(event)} $message")
            .map(_ => RecoverableError(event, error))
        case error @ SilentRecoverableError(_, _) =>
          RecoverableError(event, error).pure[F].widen[UploadingResult]
      }
    case error: NonRecoverableFailure =>
      Logger[F]
        .error(error)(s"${logMessageCommon(event)} ${error.message}")
        .map(_ => NonRecoverableError(event, error: Throwable))
  }

  private def nonRecoverableFailure(event: MinProjectInfoEvent): PartialFunction[Throwable, F[UploadingResult]] = {
    case exception: ProcessingNonRecoverableError.MalformedRepository =>
      NonRecoverableError(event, exception).pure[F].widen[UploadingResult]
    case NonFatal(exception) =>
      Logger[F]
        .error(exception)(s"${logMessageCommon(event)} ${exception.getMessage}")
        .map(_ => NonRecoverableError(event, exception))
  }

  private def logSummary(event: MinProjectInfoEvent): ((ElapsedTime, UploadingResult)) => F[Unit] = {
    case (elapsedTime, uploadingResult) =>
      val message = uploadingResult match {
        case Uploaded(_) => "was successfully processed"
        case _           => "failed to process"
      }
      Logger[F].info(s"${logMessageCommon(event)} processed in ${elapsedTime}ms: $message")
  }

  private def logMessageCommon(event: MinProjectInfoEvent): String = show"$categoryName: $event"

  private sealed trait UploadingResult extends Product with Serializable {
    val event: MinProjectInfoEvent
  }

  private sealed trait UploadingError extends UploadingResult {
    val cause: Throwable
  }

  private object UploadingResult {
    case class Uploaded(event: MinProjectInfoEvent) extends UploadingResult

    case class RecoverableError(event: MinProjectInfoEvent, cause: ProcessingRecoverableError) extends UploadingError

    case class NonRecoverableError(event: MinProjectInfoEvent, cause: Throwable) extends UploadingError
  }
}

private object EventProcessor {

  import eu.timepit.refined.auto._

  def apply[F[_]: Async: NonEmptyParallel: Parallel: Logger: MetricsRegistry: SparqlQueryTimeRecorder](
      gitLabClient: GitLabClient[F]
  ): F[EventProcessor[F]] = for {
    uploader          <- TransformationStepsRunner[F]
    accessTokenFinder <- AccessTokenFinder[F]
    stepsCreator      <- TransformationStepsCreator[F]
    entityBuilder     <- EntityBuilder(gitLabClient)
    eventsProcessingTimes <- Histogram(
                               name = "min_project_info_processing_times",
                               help = "Min project info processing times",
                               buckets = Seq(.1, .5, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000,
                                             1000000, 5000000, 10000000, 50000000, 100000000, 500000000)
                             )
    executionTimeRecorder <- ExecutionTimeRecorder[F](maybeHistogram = Some(eventsProcessingTimes))
  } yield new EventProcessorImpl(
    accessTokenFinder,
    stepsCreator,
    uploader,
    entityBuilder,
    executionTimeRecorder
  )
}
