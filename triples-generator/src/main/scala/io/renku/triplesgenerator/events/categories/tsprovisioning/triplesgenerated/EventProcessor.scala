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

package io.renku.triplesgenerator.events.categories.tsprovisioning.triplesgenerated

import cats.data.EitherT.right
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel, Parallel}
import eu.timepit.refined.auto._
import io.renku.graph.model.events.EventStatus.TriplesGenerated
import io.renku.graph.model.events.{EventProcessingTime, EventStatus}
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.logging.ExecutionTimeRecorder
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.metrics.{Histogram, MetricsRegistry}
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.events.categories.EventStatusUpdater._
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError._
import io.renku.triplesgenerator.events.categories.tsprovisioning.transformation.TransformationStepsCreator
import io.renku.triplesgenerator.events.categories.tsprovisioning.triplesuploading.TriplesUploadResult._
import io.renku.triplesgenerator.events.categories.tsprovisioning.triplesuploading.{TransformationStepsRunner, TriplesUploadResult}
import io.renku.triplesgenerator.events.categories.{EventStatusUpdater, ProcessingNonRecoverableError, ProcessingRecoverableError}
import org.typelevel.log4cats.Logger

import java.time.Duration
import scala.util.control.NonFatal

private trait EventProcessor[F[_]] {
  def process(triplesGeneratedEvent: TriplesGeneratedEvent): F[Unit]
}

private class EventProcessorImpl[F[_]: MonadThrow: Logger](
    accessTokenFinder:     AccessTokenFinder[F],
    stepsCreator:          TransformationStepsCreator[F],
    uploader:              TransformationStepsRunner[F],
    statusUpdater:         EventStatusUpdater[F],
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

  def process(event: TriplesGeneratedEvent): F[Unit] = {
    for {
      maybeAccessToken <- findAccessToken(event.project.path).recoverWith(rollback(event))
      results          <- measureExecutionTime(transformAndUpload(event)(maybeAccessToken))
      _                <- updateEventLog(results)
      _                <- logSummary(event)(results)
    } yield ()
  } recoverWith logError(event)

  private def logError(event: TriplesGeneratedEvent): PartialFunction[Throwable, F[Unit]] = {
    case NonFatal(exception) =>
      Logger[F].error(exception)(show"$categoryName: Triples Generated Event processing failure: $event")
  }

  private def transformAndUpload(
      event:                   TriplesGeneratedEvent
  )(implicit maybeAccessToken: Option[AccessToken]): F[UploadingResult] = {
    for {
      project <- buildEntity(event) leftSemiflatMap toUploadingError(event)
      result  <- right[UploadingResult](run(createSteps, project) >>= (toUploadingResult(event, _)))
    } yield result
  }.merge recoverWith nonRecoverableFailure(event)

  private def toUploadingResult(triplesGeneratedEvent: TriplesGeneratedEvent,
                                triplesUploadResult:   TriplesUploadResult
  ): F[UploadingResult] = triplesUploadResult match {
    case DeliverySuccess =>
      (Uploaded(triplesGeneratedEvent): UploadingResult)
        .pure[F]
    case RecoverableFailure(error) =>
      error match {
        case error @ LogWorthyRecoverableError(message, _) =>
          Logger[F]
            .error(error)(s"${logMessageCommon(triplesGeneratedEvent)} $message")
            .map(_ => RecoverableError(triplesGeneratedEvent, error))
        case error @ SilentRecoverableError(_, _) =>
          RecoverableError(triplesGeneratedEvent, error).pure[F].widen[UploadingResult]
      }
    case error: NonRecoverableFailure =>
      Logger[F]
        .error(error)(s"${logMessageCommon(triplesGeneratedEvent)} ${error.message}")
        .map(_ => NonRecoverableError(triplesGeneratedEvent, error: Throwable))
  }

  private def nonRecoverableFailure(
      triplesGeneratedEvent: TriplesGeneratedEvent
  ): PartialFunction[Throwable, F[UploadingResult]] = {
    case exception: ProcessingNonRecoverableError.MalformedRepository =>
      NonRecoverableError(triplesGeneratedEvent, exception).pure[F].widen[UploadingResult]
    case NonFatal(exception) =>
      Logger[F]
        .error(exception)(s"${logMessageCommon(triplesGeneratedEvent)} ${exception.getMessage}")
        .map(_ => NonRecoverableError(triplesGeneratedEvent, exception))
  }

  private def toUploadingError(
      triplesGeneratedEvent: TriplesGeneratedEvent
  ): PartialFunction[Throwable, F[UploadingResult]] = {
    case error: LogWorthyRecoverableError =>
      Logger[F]
        .error(error)(s"${logMessageCommon(triplesGeneratedEvent)} ${error.getMessage}")
        .map(_ => RecoverableError(triplesGeneratedEvent, error))
    case error: SilentRecoverableError =>
      RecoverableError(triplesGeneratedEvent, error).pure[F].widen[UploadingResult]
  }

  private lazy val updateEventLog: ((ElapsedTime, UploadingResult)) => F[Unit] = {
    case (elapsedTime, Uploaded(event)) =>
      statusUpdater
        .toTriplesStore(event.compoundEventId,
                        event.project.path,
                        EventProcessingTime(Duration ofMillis elapsedTime.value)
        )
        .recoverWith(logEventLogUpdateError(event, "done"))
    case (_, RecoverableError(event, cause)) =>
      statusUpdater
        .toFailure(
          event.compoundEventId,
          event.project.path,
          EventStatus.TransformationRecoverableFailure,
          cause,
          executionDelay = cause match {
            case _: SilentRecoverableError => ExecutionDelay(Duration.ofHours(1))
            case _ => ExecutionDelay(Duration.ofMinutes(5))
          }
        )
        .recoverWith(logEventLogUpdateError(event, "as failed recoverably"))
    case (_, NonRecoverableError(event, cause)) =>
      statusUpdater
        .toFailure(event.compoundEventId, event.project.path, EventStatus.TransformationNonRecoverableFailure, cause)
        .recoverWith(logEventLogUpdateError(event, "as failed nonrecoverably"))
  }

  private def logEventLogUpdateError(event:   TriplesGeneratedEvent,
                                     message: String
  ): PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(s"${logMessageCommon(event)} failed to mark $message in the Event Log")
  }

  private def logSummary(
      triplesGeneratedEvent: TriplesGeneratedEvent
  ): ((ElapsedTime, UploadingResult)) => F[Unit] = { case (elapsedTime, uploadingResult) =>
    val message = uploadingResult match {
      case Uploaded(_) => "was successfully uploaded"
      case _           => "failed to upload"
    }
    Logger[F].info(s"${logMessageCommon(triplesGeneratedEvent)} processed in ${elapsedTime}ms: $message")
  }

  private def logMessageCommon(event: TriplesGeneratedEvent): String = show"$categoryName: $event"

  private def rollback(
      triplesGeneratedEvent: TriplesGeneratedEvent
  ): PartialFunction[Throwable, F[Option[AccessToken]]] = { case NonFatal(exception) =>
    statusUpdater.rollback[TriplesGenerated](triplesGeneratedEvent.compoundEventId,
                                             triplesGeneratedEvent.project.path
    ) >> new Exception(
      "transformation failure -> Event rolled back",
      exception
    ).raiseError[F, Option[AccessToken]]
  }

  private sealed trait UploadingResult extends Product with Serializable {
    val event: TriplesGeneratedEvent
  }

  private sealed trait UploadingError extends UploadingResult {
    val cause: Throwable
  }

  private object UploadingResult {
    case class Uploaded(event: TriplesGeneratedEvent) extends UploadingResult

    case class RecoverableError(event: TriplesGeneratedEvent, cause: ProcessingRecoverableError) extends UploadingError

    case class NonRecoverableError(event: TriplesGeneratedEvent, cause: Throwable) extends UploadingError
  }
}

private object EventProcessor {

  def apply[F[_]: Async: NonEmptyParallel: Parallel: Logger: MetricsRegistry: SparqlQueryTimeRecorder](
      gitLabClient: GitLabClient[F]
  ): F[EventProcessor[F]] = for {
    uploader           <- TransformationStepsRunner[F]
    accessTokenFinder  <- AccessTokenFinder[F]
    triplesCurator     <- TransformationStepsCreator[F]
    eventStatusUpdater <- EventStatusUpdater(categoryName)
    eventsProcessingTimes <- Histogram(
                               name = "triples_transformation_processing_times",
                               help = "Triples transformation processing times",
                               buckets = Seq(.1, .5, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000,
                                             1000000, 5000000, 10000000, 50000000, 100000000, 500000000)
                             )
    executionTimeRecorder <- ExecutionTimeRecorder[F](maybeHistogram = Some(eventsProcessingTimes))
    entityBuilder         <- EntityBuilder(gitLabClient)
  } yield new EventProcessorImpl(
    accessTokenFinder,
    triplesCurator,
    uploader,
    eventStatusUpdater,
    entityBuilder,
    executionTimeRecorder
  )
}
