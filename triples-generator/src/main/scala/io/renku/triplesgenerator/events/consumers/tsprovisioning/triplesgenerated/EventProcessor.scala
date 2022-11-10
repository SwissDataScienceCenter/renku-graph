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

package io.renku.triplesgenerator.events.consumers
package tsprovisioning
package triplesgenerated

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
import io.renku.triplesgenerator.events.consumers.EventStatusUpdater._
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError._
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger
import transformation.TransformationStepsCreator
import triplesuploading.TriplesUploadResult._
import triplesuploading.{TransformationStepsRunner, TriplesUploadResult}

import java.time.Duration
import scala.util.control.NonFatal

private trait EventProcessor[F[_]] {
  def process(event: TriplesGeneratedEvent): F[Unit]
}

private class EventProcessorImpl[F[_]: MonadThrow: AccessTokenFinder: Logger](
    stepsCreator:          TransformationStepsCreator[F],
    uploader:              TransformationStepsRunner[F],
    statusUpdater:         EventStatusUpdater[F],
    entityBuilder:         EntityBuilder[F],
    executionTimeRecorder: ExecutionTimeRecorder[F]
) extends EventProcessor[F] {

  private val accessTokenFinder: AccessTokenFinder[F] = AccessTokenFinder[F]
  import EventUploadingResult._
  import accessTokenFinder._
  import entityBuilder._
  import executionTimeRecorder._
  import stepsCreator._
  import uploader._

  def process(event: TriplesGeneratedEvent): F[Unit] = {
    for {
      implicit0(mat: Option[AccessToken]) <- findAccessToken(event.project.path) recoverWith rollback(event)
      results                             <- measureExecutionTime(transformAndUpload(event))
      _                                   <- updateEventLog(results)
      _                                   <- logSummary(event)(results)
    } yield ()
  } recoverWith logError(event)

  private def logError(event: TriplesGeneratedEvent): PartialFunction[Throwable, F[Unit]] = {
    case NonFatal(exception) =>
      Logger[F].error(exception)(show"$categoryName: processing failure: $event")
  }

  private def transformAndUpload(
      event:      TriplesGeneratedEvent
  )(implicit mat: Option[AccessToken]): F[EventUploadingResult] = {
    for {
      project <- buildEntity(event) leftSemiflatMap toUploadingError(event)
      result  <- right[EventUploadingResult](run(createSteps, project) >>= toUploadingResult(event))
    } yield result
  }.merge recoverWith nonRecoverableFailure(event)

  private def toUploadingResult(event: TriplesGeneratedEvent): TriplesUploadResult => F[EventUploadingResult] = {
    case DeliverySuccess =>
      Uploaded(event).pure[F].widen
    case RecoverableFailure(error @ LogWorthyRecoverableError(message, _)) =>
      Logger[F]
        .error(error)(s"${logMessageCommon(event)} $message")
        .map(_ => RecoverableError(event, error))
    case RecoverableFailure(error @ SilentRecoverableError(_, _)) =>
      RecoverableError(event, error).pure[F].widen[EventUploadingResult]
    case error: NonRecoverableFailure =>
      Logger[F]
        .error(error)(s"${logMessageCommon(event)} ${error.message}")
        .map(_ => NonRecoverableError(event, error: Throwable))
  }

  private def nonRecoverableFailure(
      event: TriplesGeneratedEvent
  ): PartialFunction[Throwable, F[EventUploadingResult]] = {
    case exception: ProcessingNonRecoverableError.MalformedRepository =>
      NonRecoverableError(event, exception).pure[F].widen[EventUploadingResult]
    case NonFatal(exception) =>
      Logger[F]
        .error(exception)(s"${logMessageCommon(event)} ${exception.getMessage}")
        .map(_ => NonRecoverableError(event, exception))
  }

  private def toUploadingError(event: TriplesGeneratedEvent): PartialFunction[Throwable, F[EventUploadingResult]] = {
    case error: LogWorthyRecoverableError =>
      Logger[F]
        .error(error)(s"${logMessageCommon(event)} ${error.getMessage}")
        .map(_ => RecoverableError(event, error))
    case error: SilentRecoverableError =>
      RecoverableError(event, error).pure[F].widen[EventUploadingResult]
  }

  private lazy val updateEventLog: ((ElapsedTime, EventUploadingResult)) => F[Unit] = {
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

  private def logSummary(event: TriplesGeneratedEvent): ((ElapsedTime, EventUploadingResult)) => F[Unit] = {
    case (elapsedTime, uploadingResult) =>
      val message = uploadingResult match {
        case Uploaded(_) => "success"
        case _           => "failure"
      }
      Logger[F].info(s"${logMessageCommon(event)} processed in ${elapsedTime}ms: $message")
  }

  private def logMessageCommon(event: TriplesGeneratedEvent): String = show"$categoryName: $event"

  private def rollback(
      triplesGeneratedEvent: TriplesGeneratedEvent
  ): PartialFunction[Throwable, F[Option[AccessToken]]] = { case NonFatal(exception) =>
    statusUpdater.rollback[TriplesGenerated](triplesGeneratedEvent.compoundEventId,
                                             triplesGeneratedEvent.project.path
    ) >> new Exception("transformation failure -> Event rolled back", exception).raiseError[F, Option[AccessToken]]
  }

  private sealed trait EventUploadingResult extends UploadingResult {
    val event: TriplesGeneratedEvent
  }

  private sealed trait EventUploadingError extends EventUploadingResult {
    val cause: Throwable
  }

  private object EventUploadingResult {
    case class Uploaded(event: TriplesGeneratedEvent) extends EventUploadingResult with UploadingResult.Uploaded

    case class RecoverableError(event: TriplesGeneratedEvent, cause: ProcessingRecoverableError)
        extends EventUploadingError
        with UploadingResult.RecoverableError

    case class NonRecoverableError(event: TriplesGeneratedEvent, cause: Throwable)
        extends EventUploadingError
        with UploadingResult.NonRecoverableError
  }
}

private object EventProcessor {

  def apply[F[
      _
  ]: Async: NonEmptyParallel: Parallel: GitLabClient: AccessTokenFinder: Logger: MetricsRegistry: SparqlQueryTimeRecorder]
      : F[EventProcessor[F]] = for {
    uploader           <- TransformationStepsRunner[F]
    stepsCreator       <- TransformationStepsCreator[F]
    eventStatusUpdater <- EventStatusUpdater(categoryName)
    eventsProcessingTimes <- Histogram(
                               name = "triples_transformation_processing_times",
                               help = "Triples transformation processing times",
                               buckets = Seq(.1, .5, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000,
                                             1000000, 5000000, 10000000, 50000000, 100000000, 500000000)
                             )
    executionTimeRecorder <- ExecutionTimeRecorder[F](maybeHistogram = Some(eventsProcessingTimes))
    entityBuilder         <- EntityBuilder[F]
  } yield new EventProcessorImpl(stepsCreator, uploader, eventStatusUpdater, entityBuilder, executionTimeRecorder)
}
