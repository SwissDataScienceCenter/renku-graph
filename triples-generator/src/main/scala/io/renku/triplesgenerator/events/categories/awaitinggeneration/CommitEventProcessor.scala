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

package io.renku.triplesgenerator.events.categories.awaitinggeneration

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.graph.model.events.EventStatus.{GenerationNonRecoverableFailure, GenerationRecoverableFailure}
import ch.datascience.graph.model.events.{CompoundEventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.metrics.MetricsRegistry
import io.prometheus.client.Histogram
import io.renku.jsonld.JsonLD
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.EventStatusUpdater
import io.renku.triplesgenerator.events.categories.EventStatusUpdater._
import io.renku.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.TriplesGenerator
import org.typelevel.log4cats.Logger

import java.time.Duration
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private trait EventProcessor[Interpretation[_]] {
  def process(event: CommitEvent): Interpretation[Unit]
}

private class CommitEventProcessor[Interpretation[_]: MonadThrow](
    accessTokenFinder:       AccessTokenFinder[Interpretation],
    triplesGenerator:        TriplesGenerator[Interpretation],
    statusUpdater:           EventStatusUpdater[Interpretation],
    logger:                  Logger[Interpretation],
    allEventsTimeRecorder:   ExecutionTimeRecorder[Interpretation],
    singleEventTimeRecorder: ExecutionTimeRecorder[Interpretation]
) extends EventProcessor[Interpretation] {

  import AccessTokenFinder._
  import TriplesGenerationResult._
  import accessTokenFinder._
  import triplesGenerator._

  def process(event: CommitEvent): Interpretation[Unit] = allEventsTimeRecorder.measureExecutionTime {
    for {
      maybeAccessToken <- findAccessToken(event.project.path) recoverWith rollbackEvent(event)
      uploadingResult  <- generateAndUpdateStatus(event)(maybeAccessToken)
    } yield uploadingResult
  } flatMap logSummary recoverWith logError(event)

  private def logError(event: CommitEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"${logMessageCommon(event)}: commit Event processing failure")
  }

  private def generateAndUpdateStatus(
      commit:                  CommitEvent
  )(implicit maybeAccessToken: Option[AccessToken]): Interpretation[TriplesGenerationResult] = EitherT {
    singleEventTimeRecorder
      .measureExecutionTime(generateTriples(commit).value)
      .map(toTriplesGenerated(commit))
  }.leftSemiflatMap(toRecoverableError(commit))
    .merge
    .recoverWith(toNonRecoverableFailure(commit))
    .flatTap(updateEventLog)

  private def toTriplesGenerated(commit: CommitEvent): (
      (ElapsedTime, Either[ProcessingRecoverableError, JsonLD])
  ) => Either[ProcessingRecoverableError, TriplesGenerationResult] = { case (elapsedTime, maybeTriples) =>
    maybeTriples.map { triples =>
      TriplesGenerated(
        commit,
        triples,
        EventProcessingTime(Duration ofMillis elapsedTime.value)
      ): TriplesGenerationResult
    }
  }

  private def updateEventLog(uploadingResults: TriplesGenerationResult): Interpretation[Unit] = {
    uploadingResults match {
      case TriplesGenerated(commit, triples, processingTime) =>
        statusUpdater.toTriplesGenerated(CompoundEventId(commit.eventId, commit.project.id),
                                         commit.project.path,
                                         triples,
                                         processingTime
        )
      case RecoverableError(commit, message) =>
        statusUpdater.toFailure(CompoundEventId(commit.eventId, commit.project.id),
                                commit.project.path,
                                EventStatus.GenerationRecoverableFailure,
                                message
        )
      case NonRecoverableError(commit, cause) =>
        statusUpdater.toFailure(CompoundEventId(commit.eventId, commit.project.id),
                                commit.project.path,
                                EventStatus.GenerationNonRecoverableFailure,
                                cause
        )
    }
  } recoverWith logEventLogUpdateError(uploadingResults)

  private def logEventLogUpdateError(
      triplesGenerationResult: TriplesGenerationResult
  ): PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger
      .error(exception)(
        s"${logMessageCommon(triplesGenerationResult.commit)} failed to mark as $triplesGenerationResult in the Event Log"
      )
  }

  private def toRecoverableError(
      commit: CommitEvent
  ): ProcessingRecoverableError => Interpretation[TriplesGenerationResult] = { error =>
    logger
      .error(error)(s"${logMessageCommon(commit)} ${error.getMessage}")
      .map(_ => RecoverableError(commit, error): TriplesGenerationResult)
  }

  private def toNonRecoverableFailure(
      commit: CommitEvent
  ): PartialFunction[Throwable, Interpretation[TriplesGenerationResult]] = { case NonFatal(exception) =>
    logger
      .error(exception)(s"${logMessageCommon(commit)} failed")
      .map(_ => NonRecoverableError(commit, exception): TriplesGenerationResult)
  }

  private def logSummary: ((ElapsedTime, TriplesGenerationResult)) => Interpretation[Unit] = {
    case (elapsedTime, uploadingResult @ TriplesGenerated(_, _, _)) =>
      logger.info(s"${logMessageCommon(uploadingResult.commit)} processed in ${elapsedTime}ms")
    case _ => ().pure[Interpretation]
  }

  private def rollbackEvent(commit: CommitEvent): PartialFunction[Throwable, Interpretation[Option[AccessToken]]] = {
    case NonFatal(exception) =>
      statusUpdater
        .rollback[EventStatus.New](commit.compoundEventId, commit.project.path)
        .flatMap(_ => new Exception(s"$categoryName: processing failure -> Event rolled back", exception).raiseError)
  }

  private sealed trait TriplesGenerationResult extends Product with Serializable {
    val commit: CommitEvent
  }
  private sealed trait GenerationError extends TriplesGenerationResult {
    val cause: Throwable
  }
  private object TriplesGenerationResult {
    case class TriplesGenerated(commit: CommitEvent, payload: JsonLD, processingTime: EventProcessingTime)
        extends TriplesGenerationResult {
      override lazy val toString: String = TriplesGenerated.toString()
    }

    case class RecoverableError(commit: CommitEvent, cause: Throwable) extends GenerationError {
      override lazy val toString: String = GenerationRecoverableFailure.toString
    }

    case class NonRecoverableError(commit: CommitEvent, cause: Throwable) extends GenerationError {
      override lazy val toString: String = GenerationNonRecoverableFailure.toString()
    }
  }
}

private object IOCommitEventProcessor {

  private[events] lazy val eventsProcessingTimesBuilder =
    Histogram
      .build()
      .name("triples_generation_processing_times")
      .help("Triples generation processing times")
      .buckets(.1, .5, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000, 10000000,
        50000000, 100000000, 500000000)

  def apply(
      metricsRegistry: MetricsRegistry[IO]
  )(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO],
      logger:           Logger[IO]
  ): IO[CommitEventProcessor[IO]] = for {
    triplesGenerator        <- TriplesGenerator()
    accessTokenFinder       <- AccessTokenFinder(logger)
    eventStatusUpdater      <- EventStatusUpdater(categoryName)
    eventsProcessingTimes   <- metricsRegistry.register[Histogram, Histogram.Builder](eventsProcessingTimesBuilder)
    allEventsTimeRecorder   <- ExecutionTimeRecorder[IO](logger, maybeHistogram = Some(eventsProcessingTimes))
    singleEventTimeRecorder <- ExecutionTimeRecorder[IO](logger, maybeHistogram = None)
  } yield new CommitEventProcessor(
    accessTokenFinder,
    triplesGenerator,
    eventStatusUpdater,
    logger,
    allEventsTimeRecorder,
    singleEventTimeRecorder
  )
}
