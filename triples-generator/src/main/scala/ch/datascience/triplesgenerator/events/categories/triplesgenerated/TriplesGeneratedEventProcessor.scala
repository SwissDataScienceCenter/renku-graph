/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated

import cats.MonadError
import cats.data.EitherT.right
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.graph.model.events.EventStatus.TriplesGenerated
import ch.datascience.graph.model.events.{EventProcessingTime, EventStatus}
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.events.categories.EventStatusUpdater
import ch.datascience.triplesgenerator.events.categories.EventStatusUpdater._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.IOTriplesCurator.CurationRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.{IOTriplesCurator, TriplesTransformer}
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplesuploading.TriplesUploadResult._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplesuploading.{IOUploader, TriplesUploadResult, Uploader}
import io.prometheus.client.Histogram
import org.typelevel.log4cats.Logger

import java.time.Duration
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private trait EventProcessor[Interpretation[_]] {
  def process(
      triplesGeneratedEvent: TriplesGeneratedEvent
  ): Interpretation[Unit]
}

private class TriplesGeneratedEventProcessor[Interpretation[_]: MonadError[*[_], Throwable]](
    accessTokenFinder:     AccessTokenFinder[Interpretation],
    triplesCurator:        TriplesTransformer[Interpretation],
    uploader:              Uploader[Interpretation],
    statusUpdater:         EventStatusUpdater[Interpretation],
    logger:                Logger[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation]
) extends EventProcessor[Interpretation] {

  import AccessTokenFinder._
  import UploadingResult._
  import accessTokenFinder._
  import executionTimeRecorder._
  import triplesCurator._
  import uploader._

  def process(triplesGeneratedEvent: TriplesGeneratedEvent): Interpretation[Unit] = {
    for {
      maybeAccessToken <- findAccessToken(triplesGeneratedEvent.project.path)
                            .recoverWith(rollback(triplesGeneratedEvent))
      results <- measureExecutionTime(transformAndUpload(triplesGeneratedEvent)(maybeAccessToken))
      _       <- updateEventLog(results)
      _       <- logSummary(triplesGeneratedEvent)(results)
    } yield ()
  } recoverWith logError(triplesGeneratedEvent)

  private def logError(event: TriplesGeneratedEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(
        s"$categoryName: Triples Generated Event processing failure: ${event.compoundEventId}, projectPath: ${event.project.path}"
      )
  }

  private def transformAndUpload(
      triplesGeneratedEvent:   TriplesGeneratedEvent
  )(implicit maybeAccessToken: Option[AccessToken]): Interpretation[UploadingResult] = {
    for {
      curatedTriples <-
        transform(triplesGeneratedEvent).leftSemiflatMap(
          toUploadingError(triplesGeneratedEvent)
        )
      result <- right[UploadingResult](upload(curatedTriples).flatMap(toUploadingResult(triplesGeneratedEvent, _)))
    } yield result
  }.merge recoverWith nonRecoverableFailure(triplesGeneratedEvent)

  private def toUploadingResult(triplesGeneratedEvent: TriplesGeneratedEvent,
                                triplesUploadResult:   TriplesUploadResult
  ): Interpretation[UploadingResult] = triplesUploadResult match {
    case DeliverySuccess =>
      (Uploaded(triplesGeneratedEvent): UploadingResult)
        .pure[Interpretation]
    case error @ RecoverableFailure(message) =>
      logger
        .error(error)(
          s"${logMessageCommon(triplesGeneratedEvent)} $message"
        )
        .map(_ => RecoverableError(triplesGeneratedEvent, error))
    case error @ InvalidTriplesFailure(message) =>
      logger
        .error(error)(
          s"${logMessageCommon(triplesGeneratedEvent)} $message"
        )
        .map(_ => NonRecoverableError(triplesGeneratedEvent, error: Throwable))
    case error @ InvalidUpdatesFailure(message) =>
      logger
        .error(error)(
          s"${logMessageCommon(triplesGeneratedEvent)} $message"
        )
        .map(_ => NonRecoverableError(triplesGeneratedEvent, error: Throwable))
  }

  private def nonRecoverableFailure(
      triplesGeneratedEvent: TriplesGeneratedEvent
  ): PartialFunction[Throwable, Interpretation[UploadingResult]] = { case NonFatal(exception) =>
    logger
      .error(exception)(s"${logMessageCommon(triplesGeneratedEvent)} ${exception.getMessage}")
      .map(_ => NonRecoverableError(triplesGeneratedEvent, exception))
  }

  private def toUploadingError(
      triplesGeneratedEvent: TriplesGeneratedEvent
  ): PartialFunction[Throwable, Interpretation[UploadingResult]] = {
    case curationError @ CurationRecoverableError(message, _) =>
      logger
        .error(curationError)(s"${logMessageCommon(triplesGeneratedEvent)} $message")
        .map(_ => RecoverableError(triplesGeneratedEvent, curationError))
  }

  private lazy val updateEventLog: ((ElapsedTime, UploadingResult)) => Interpretation[Unit] = {
    case (elapsedTime, Uploaded(event)) =>
      statusUpdater
        .toTriplesStore(event.compoundEventId, EventProcessingTime(Duration ofMillis elapsedTime.value))
        .recoverWith(logEventLogUpdateError(event, "done"))
    case (_, RecoverableError(event, cause)) =>
      statusUpdater
        .toFailure(event.compoundEventId, EventStatus.TransformationRecoverableFailure, cause)
        .recoverWith(logEventLogUpdateError(event, "as failed recoverably"))
    case (_, NonRecoverableError(event, cause)) =>
      statusUpdater
        .toFailure(event.compoundEventId, EventStatus.TransformationNonRecoverableFailure, cause)
        .recoverWith(logEventLogUpdateError(event, "as failed nonrecoverably"))
  }

  private def logEventLogUpdateError(event:   TriplesGeneratedEvent,
                                     message: String
  ): PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)(s"${logMessageCommon(event)} failed to mark $message in the Event Log")
  }

  private def logSummary(
      triplesGeneratedEvent: TriplesGeneratedEvent
  ): ((ElapsedTime, UploadingResult)) => Interpretation[Unit] = { case (elapsedTime, uploadingResult) =>
    val message = uploadingResult match {
      case Uploaded(_) => "was successfully uploaded"
      case _           => "failed to upload"
    }
    logger.info(s"${logMessageCommon(triplesGeneratedEvent)} processed in ${elapsedTime}ms: $message")
  }

  private def logMessageCommon(event: TriplesGeneratedEvent): String =
    s"$categoryName: ${event.compoundEventId}, projectPath: ${event.project.path}"

  private def rollback(
      triplesGeneratedEvent: TriplesGeneratedEvent
  ): PartialFunction[Throwable, Interpretation[Option[AccessToken]]] = { case NonFatal(exception) =>
    statusUpdater.rollback[TriplesGenerated](triplesGeneratedEvent.compoundEventId) >> new Exception(
      "transformation failure -> Event rolled back",
      exception
    ).raiseError[Interpretation, Option[AccessToken]]
  }

  private sealed trait UploadingResult extends Product with Serializable {
    val event: TriplesGeneratedEvent
  }

  private sealed trait UploadingError extends UploadingResult {
    val cause: Throwable
  }

  private object UploadingResult {
    case class Uploaded(event: TriplesGeneratedEvent) extends UploadingResult

    case class RecoverableError(event: TriplesGeneratedEvent, cause: Throwable) extends UploadingError

    case class NonRecoverableError(event: TriplesGeneratedEvent, cause: Throwable) extends UploadingError
  }
}

private object IOTriplesGeneratedEventProcessor {

  import ch.datascience.config.GitLab
  import ch.datascience.control.Throttler

  private[events] lazy val eventsProcessingTimesBuilder =
    Histogram
      .build()
      .name("triples_transformation_processing_times")
      .help("Triples transformation processing times")
      .buckets(.1, .5, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000, 10000000,
        50000000, 100000000, 500000000)

  def apply(
      metricsRegistry: MetricsRegistry[IO],
      gitLabThrottler: Throttler[IO, GitLab],
      timeRecorder:    SparqlQueryTimeRecorder[IO],
      logger:          Logger[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[TriplesGeneratedEventProcessor[IO]] =
    for {
      uploader              <- IOUploader(logger, timeRecorder)
      accessTokenFinder     <- AccessTokenFinder(logger)
      triplesCurator        <- IOTriplesCurator(gitLabThrottler, logger, timeRecorder)
      eventStatusUpdater    <- EventStatusUpdater(categoryName, logger)
      eventsProcessingTimes <- metricsRegistry.register[Histogram, Histogram.Builder](eventsProcessingTimesBuilder)
      executionTimeRecorder <- ExecutionTimeRecorder[IO](logger, maybeHistogram = Some(eventsProcessingTimes))
    } yield new TriplesGeneratedEventProcessor(
      accessTokenFinder,
      triplesCurator,
      uploader,
      eventStatusUpdater,
      logger,
      executionTimeRecorder
    )
}
