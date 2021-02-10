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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration

import cats.MonadError
import cats.data.NonEmptyList
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
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.EventHandler.categoryName
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.GenerationResult._
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.TriplesGenerator
import ch.datascience.triplesgenerator.events.categories.{EventStatusUpdater, IOEventStatusUpdater}
import io.chrisdavenport.log4cats.Logger
import io.prometheus.client.Histogram

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private trait EventProcessor[Interpretation[_]] {
  def process(eventId:              CompoundEventId,
              events:               NonEmptyList[CommitEvent],
              currentSchemaVersion: SchemaVersion
  ): Interpretation[Unit]
}

private class CommitEventProcessor[Interpretation[_]](
    accessTokenFinder:     AccessTokenFinder[Interpretation],
    triplesGenerator:      TriplesGenerator[Interpretation],
    eventStatusUpdater:    EventStatusUpdater[Interpretation],
    logger:                Logger[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable])
    extends EventProcessor[Interpretation] {

  import IOAccessTokenFinder._
  import TriplesGenerationResult._
  import accessTokenFinder._
  import eventStatusUpdater._
  import executionTimeRecorder._
  import triplesGenerator._

  def process(eventId:              CompoundEventId,
              events:               NonEmptyList[CommitEvent],
              currentSchemaVersion: SchemaVersion
  ): Interpretation[Unit] =
    measureExecutionTime {
      for {
        maybeAccessToken <- findAccessToken(events.head.project.path) recoverWith rollback(events.head)
        uploadingResults <- generateAllTriples(events, currentSchemaVersion)(maybeAccessToken)
      } yield uploadingResults
    } flatMap logSummary recoverWith logError(eventId, events.head.project.path)

  private def logError(eventId:     CompoundEventId,
                       projectPath: projects.Path
  ): PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)(s"$categoryName: Commit Event processing failure: $eventId, projectPath: $projectPath")
    ME.unit
  }

  private def generateAllTriples(
      commits:                 NonEmptyList[CommitEvent],
      currentSchemaVersion:    SchemaVersion
  )(implicit maybeAccessToken: Option[AccessToken]): Interpretation[NonEmptyList[TriplesGenerationResult]] =
    commits
      .map(commit => generateAndUpdateStatus(commit, currentSchemaVersion))
      .sequence

  private def generateAndUpdateStatus(
      commit:                  CommitEvent,
      currentSchemaVersion:    SchemaVersion
  )(implicit maybeAccessToken: Option[AccessToken]): Interpretation[TriplesGenerationResult] =
    generateTriples(commit)
      .map {
        case MigrationEvent      => Skipped(commit, MigrationEvent.toString)
        case Triples(rawTriples) => TriplesGenerated(commit, rawTriples, currentSchemaVersion)
      }
      .widen[TriplesGenerationResult]
      .leftSemiflatMap(toRecoverableError(commit))
      .merge
      .recoverWith(toNonRecoverableFailure(commit))
      .flatTap(updateEventLog)

  private def updateEventLog(uploadingResults: TriplesGenerationResult): Interpretation[Unit] = {
    uploadingResults match {
      case TriplesGenerated(commit, triples, schemaVersion) =>
        markTriplesGenerated(CompoundEventId(commit.eventId, commit.project.id), triples, schemaVersion)
      case Skipped(commit, message) =>
        markEventSkipped(CompoundEventId(commit.eventId, commit.project.id), message)
      case RecoverableError(commit, message) =>
        markEventFailedRecoverably(CompoundEventId(commit.eventId, commit.project.id), message)
      case NonRecoverableError(commit, cause) =>
        markEventFailedNonRecoverably(CompoundEventId(commit.eventId, commit.project.id), cause)
    }
  } recoverWith logEventLogUpdateError(uploadingResults)

  private def logEventLogUpdateError(
      triplesGenerationResult: TriplesGenerationResult
  ): PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger
      .error(exception)(
        s"${logMessageCommon(triplesGenerationResult.commit)} failed to mark as TriplesGenerated in the Event Log"
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

  private def logSummary: ((ElapsedTime, NonEmptyList[TriplesGenerationResult])) => Interpretation[Unit] = {
    case (elapsedTime, uploadingResults) =>
      val (processed, skipped, failed) =
        uploadingResults.foldLeft((List.empty[TriplesGenerated], List.empty[Skipped], List.empty[GenerationError])) {
          case ((allProcessed, allSkipped, allFailed), processed @ TriplesGenerated(_, _, _)) =>
            (allProcessed :+ processed, allSkipped, allFailed)
          case ((allProcessed, allSkipped, allFailed), skipped @ Skipped(_, _)) =>
            (allProcessed, allSkipped :+ skipped, allFailed)
          case ((allProcessed, allSkipped, allFailed), failed: GenerationError) =>
            (allProcessed, allSkipped, allFailed :+ failed)
        }
      logger.info(
        s"${logMessageCommon(uploadingResults.head.commit)} processed in ${elapsedTime}ms: " +
          s"${uploadingResults.size} commits, " +
          s"${processed.size} successfully processed, " +
          s"${skipped.size} skipped, " +
          s"${failed.size} failed"
      )
  }

  private def logMessageCommon(event: CommitEvent): String =
    s"$categoryName: Commit Event id: ${event.compoundEventId}, ${event.project.path}"

  private def rollback(commit: CommitEvent): PartialFunction[Throwable, Interpretation[Option[AccessToken]]] = {
    case NonFatal(exception) =>
      markEventNew(commit.compoundEventId)
        .flatMap(_ =>
          ME.raiseError(new Exception(s"$categoryName: processing failure -> Event rolled back", exception))
        )
  }

  private sealed trait TriplesGenerationResult extends Product with Serializable {
    val commit: CommitEvent
  }
  private sealed trait GenerationError extends TriplesGenerationResult {
    val cause: Throwable
  }
  private object TriplesGenerationResult {
    case class TriplesGenerated(commit: CommitEvent, triples: JsonLDTriples, schemaVersion: SchemaVersion)
        extends TriplesGenerationResult
    case class Skipped(commit: CommitEvent, message: String) extends TriplesGenerationResult
    case class RecoverableError(commit: CommitEvent, cause: Throwable) extends GenerationError
    case class NonRecoverableError(commit: CommitEvent, cause: Throwable) extends GenerationError
  }
}

private object IOCommitEventProcessor {
  import ch.datascience.config.GitLab
  import ch.datascience.control.Throttler

  private[events] lazy val eventsProcessingTimesBuilder =
    Histogram
      .build()
      .name("triples_generation_processing_times")
      .help("Triples generation processing times")
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
  ): IO[CommitEventProcessor[IO]] =
    for {
      triplesGenerator      <- TriplesGenerator()
      accessTokenFinder     <- IOAccessTokenFinder(logger)
      eventStatusUpdater    <- IOEventStatusUpdater(logger)
      eventsProcessingTimes <- metricsRegistry.register[Histogram, Histogram.Builder](eventsProcessingTimesBuilder)
      executionTimeRecorder <- ExecutionTimeRecorder[IO](logger, maybeHistogram = Some(eventsProcessingTimes))
    } yield new CommitEventProcessor(
      accessTokenFinder,
      triplesGenerator,
      eventStatusUpdater,
      logger,
      executionTimeRecorder
    )
}
