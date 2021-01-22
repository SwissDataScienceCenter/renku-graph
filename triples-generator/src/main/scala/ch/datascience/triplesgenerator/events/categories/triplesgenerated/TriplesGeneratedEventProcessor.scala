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
import cats.data.EitherT
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.graph.model.{SchemaVersion, projects}
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.{JsonLDTriples, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.IOTriplesCurator.CurationRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.{IOTriplesCurator, TriplesTransformer}
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplesuploading.TriplesUploadResult._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplesuploading.{IOUploader, TriplesUploadResult, Uploader, UploaderImpl}
import ch.datascience.triplesgenerator.events.categories.{EventStatusUpdater, IOEventStatusUpdater}
import io.chrisdavenport.log4cats.Logger
import io.prometheus.client.Histogram

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private trait EventProcessor[Interpretation[_]] {
  def process(
      triplesGeneratedEvent: TriplesGeneratedEvent,
      currentSchemaVersion:  SchemaVersion
  ): Interpretation[Unit]
}

private class TriplesGeneratedEventProcessor[Interpretation[_]](
    accessTokenFinder:     AccessTokenFinder[Interpretation],
    triplesCurator:        TriplesTransformer[Interpretation],
    uploader:              Uploader[Interpretation],
    eventStatusUpdater:    EventStatusUpdater[Interpretation],
    logger:                Logger[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable])
    extends EventProcessor[Interpretation] {

  import IOAccessTokenFinder._
  import UploadingResult._
  import accessTokenFinder._
  import eventStatusUpdater._
  import executionTimeRecorder._
  import triplesCurator._
  import uploader._

  def process(triplesGeneratedEvent: TriplesGeneratedEvent, currentSchemaVersion: SchemaVersion): Interpretation[Unit] =
    measureExecutionTime {
      for {
        maybeAccessToken <- findAccessToken(triplesGeneratedEvent.project.path) recoverWith rollback(
                              triplesGeneratedEvent,
                              currentSchemaVersion
                            )
        results <-
          transformTriplesAndUpload(triplesGeneratedEvent, currentSchemaVersion)(maybeAccessToken)
        uploadingResults <-
          updateEventLog(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.project.path, results)
      } yield uploadingResults
    }.flatMap(
      logSummary(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.project.path)
    ) recoverWith logError(
      triplesGeneratedEvent.compoundEventId,
      triplesGeneratedEvent.project.path
    )

  private def logError(eventId:     CompoundEventId,
                       projectPath: projects.Path
  ): PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)(s"Triples Generated Event processing failure: $eventId, projectPath: $projectPath")
    ME.unit
  }
  private def transformTriplesAndUpload(
      triplesGeneratedEvent:   TriplesGeneratedEvent,
      currentSchemaVersion:    SchemaVersion
  )(implicit maybeAccessToken: Option[AccessToken]): Interpretation[UploadingResult] = {
    for {
      curatedTriples <- transform(triplesGeneratedEvent)
      result <- EitherT.liftF[Interpretation, ProcessingRecoverableError, UploadingResult](
                  upload(curatedTriples).map(toUploadingResult(triplesGeneratedEvent, _))
                )
    } yield result
  }.leftSemiflatMap(toRecoverableError(triplesGeneratedEvent))
    .merge
    .recoverWith(nonRecoverableFailure(triplesGeneratedEvent))

  private def toUploadingResult(triplesGeneratedEvent: TriplesGeneratedEvent,
                                triplesUploadResult:   TriplesUploadResult
  ): UploadingResult = triplesUploadResult match {
    case DeliverySuccess => Uploaded(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.triples)
    case error @ RecoverableFailure(message) =>
      logger.error(
        s"${logMessageCommon(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.project.path)} $message"
      )
      RecoverableError(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.triples, error)
    case error @ InvalidTriplesFailure(message) =>
      logger.error(
        s"${logMessageCommon(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.project.path)} $message"
      )
      NonRecoverableError(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.triples, error: Throwable)
    case error @ InvalidUpdatesFailure(message) =>
      logger.error(
        s"${logMessageCommon(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.project.path)} $message"
      )
      NonRecoverableError(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.triples, error: Throwable)
  }

  private def toRecoverableError(
      triplesGeneratedEvent: TriplesGeneratedEvent
  ): ProcessingRecoverableError => Interpretation[UploadingResult] = {
    case error @ CurationRecoverableError(_, _) =>
      logger
        .error(error)(
          s"${logMessageCommon(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.project.path)} ${error.getMessage}"
        )
        .map(_ => RecoverableError(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.triples, error))
    case error =>
      logger
        .error(error)(
          s"${logMessageCommon(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.project.path)} ${error.getMessage}"
        )
        .map(_ => RecoverableError(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.triples, error))
  }

  private def nonRecoverableFailure(
      triplesGeneratedEvent: TriplesGeneratedEvent
  ): PartialFunction[Throwable, Interpretation[UploadingResult]] = { case NonFatal(exception) =>
    logger
      .error(exception)(
        s"${logMessageCommon(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.project.path)} ${exception.getMessage}"
      )
      .map(_ => NonRecoverableError(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.triples, exception))
  }

  private def updateEventLog(eventId:          CompoundEventId,
                             projectPath:      projects.Path,
                             uploadingResults: UploadingResult
  ) = {
    for {
      _ <- uploadingResults match {
             case Uploaded(eventId, _)         => markEventDone(eventId)
             case Skipped(eventId, _, message) => markEventSkipped(eventId, message)
             case RecoverableError(eventId, _, message) =>
               markEventTransformationFailedRecoverably(eventId, message)
             case NonRecoverableError(eventId, _, cause) => markEventTransformationFailedNonRecoverably(eventId, cause)
           }
    } yield uploadingResults
  } recoverWith logEventLogUpdateError(eventId, projectPath, uploadingResults)

  private def logEventLogUpdateError(
      eventId:          CompoundEventId,
      projectPath:      projects.Path,
      uploadingResults: UploadingResult
  ): PartialFunction[Throwable, Interpretation[UploadingResult]] = { case NonFatal(exception) =>
    logger
      .error(exception)(
        s"${logMessageCommon(eventId, projectPath)} failed to mark as TriplesStore in the Event Log"
      )
      .map(_ => uploadingResults)
  }

  private def logSummary(eventId:     CompoundEventId,
                         projectPath: projects.Path
  ): ((ElapsedTime, UploadingResult)) => Interpretation[Unit] = { case (elapsedTime, uploadingResult) =>
    val message = uploadingResult match {
      case Uploaded(_, _) => "was successfully uploaded"
      case _              => "failed to upload"
    }
    logger.info(s"${logMessageCommon(eventId, projectPath)} processed in ${elapsedTime}ms: ${message}")
  }

  private def logMessageCommon(eventId: CompoundEventId, projectPath: projects.Path): String =
    s"Triples Generated Event id: $eventId, $projectPath"

  private def rollback(triplesGeneratedEvent: TriplesGeneratedEvent,
                       schemaVersion:         SchemaVersion
  ): PartialFunction[Throwable, Interpretation[Option[AccessToken]]] = { case NonFatal(exception) =>
    markTriplesGenerated(triplesGeneratedEvent.compoundEventId, triplesGeneratedEvent.triples, schemaVersion)
      .flatMap(_ => ME.raiseError(new Exception("transformation failure -> Event rolled back", exception)))
  }

  private sealed trait UploadingResult extends Product with Serializable {
    val triples: JsonLDTriples
  }
  private sealed trait UploadingError extends UploadingResult {
    val cause: Throwable
  }
  private object UploadingResult {
    case class Uploaded(eventId: CompoundEventId, triples: JsonLDTriples) extends UploadingResult
    case class Skipped(eventId: CompoundEventId, triples: JsonLDTriples, message: String) extends UploadingResult
    case class RecoverableError(eventId: CompoundEventId, triples: JsonLDTriples, cause: Throwable)
        extends UploadingError
    case class NonRecoverableError(eventId: CompoundEventId, triples: JsonLDTriples, cause: Throwable)
        extends UploadingError
  }
}

private object IOTriplesGeneratedEventProcessor {
  import ch.datascience.config.GitLab
  import ch.datascience.control.Throttler

  private[events] lazy val eventsProcessingTimesBuilder =
    Histogram
      .build()
      .name("events_processing_times")
      .help("Triples Generated Events processing times")
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
      accessTokenFinder     <- IOAccessTokenFinder(logger)
      triplesCurator        <- IOTriplesCurator(gitLabThrottler, logger, timeRecorder)
      eventStatusUpdater    <- IOEventStatusUpdater(logger)
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
