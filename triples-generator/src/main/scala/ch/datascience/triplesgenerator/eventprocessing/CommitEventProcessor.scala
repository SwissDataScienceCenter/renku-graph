/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing

import cats.MonadError
import cats.data.{EitherT, NonEmptyList}
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventStatus._
import ch.datascience.dbeventlog.commands._
import ch.datascience.dbeventlog.{EventLogDB, EventMessage}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.graph.model.projects
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.{IOTriplesCurator, TriplesCurator}
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator
import ch.datascience.triplesgenerator.eventprocessing.triplesuploading.TriplesUploadResult._
import ch.datascience.triplesgenerator.eventprocessing.triplesuploading.{IOUploader, TriplesUploadResult, Uploader}
import io.chrisdavenport.log4cats.Logger
import io.prometheus.client.Histogram

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

private trait EventProcessor[Interpretation[_]] {
  def process(eventId: CompoundEventId, events: NonEmptyList[CommitEvent]): Interpretation[Unit]
}

private class CommitEventProcessor[Interpretation[_]](
    accessTokenFinder:     AccessTokenFinder[Interpretation],
    triplesGenerator:      TriplesGenerator[Interpretation],
    triplesCurator:        TriplesCurator[Interpretation],
    uploader:              Uploader[Interpretation],
    eventStatusUpdater:    EventStatusUpdater[Interpretation],
    eventLogMarkNew:       EventLogMarkNew[Interpretation],
    eventLogMarkFailed:    EventLogMarkFailed[Interpretation],
    logger:                Logger[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable])
    extends EventProcessor[Interpretation] {

  import CommitEventProcessor._
  import IOAccessTokenFinder._
  import UploadingResult._
  import accessTokenFinder._
  import eventLogMarkFailed._
  import eventLogMarkNew._
  import eventStatusUpdater._
  import executionTimeRecorder._
  import triplesCurator._
  import triplesGenerator._
  import uploader._

  def process(eventId: CompoundEventId, events: NonEmptyList[CommitEvent]): Interpretation[Unit] =
    measureExecutionTime {
      for {
        maybeAccessToken <- findAccessToken(events.head.project.id) recoverWith rollback(events.head)
        uploadingResults <- allToTriplesAndUpload(events)(maybeAccessToken)
      } yield uploadingResults
    } flatMap logSummary recoverWith logError(eventId, events.head.project.path)

  private def logError(eventId:     CompoundEventId,
                       projectPath: projects.Path): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"Commit Event processing failure: $eventId, projectPath: $projectPath")
      ME.unit
  }

  private def allToTriplesAndUpload(
      commits:                 NonEmptyList[CommitEvent]
  )(implicit maybeAccessToken: Option[AccessToken]): Interpretation[NonEmptyList[UploadingResult]] =
    commits
      .map(toTriplesAndUpload)
      .sequence
      .flatMap(updateEventLog)

  private def toTriplesAndUpload(
      commit:                  CommitEvent
  )(implicit maybeAccessToken: Option[AccessToken]): Interpretation[UploadingResult] = {
    for {
      rawTriples     <- generateTriples(commit)
      curatedTriples <- curate(commit, rawTriples)
      result         <- EitherT.right[ProcessingRecoverableError](upload(curatedTriples) map toUploadingResult(commit))
    } yield result
  }.value map (_.fold(toRecoverableError(commit), identity)) recoverWith nonRecoverableFailure(commit)

  private def toUploadingResult(commit: CommitEvent): TriplesUploadResult => UploadingResult = {
    case DeliverySuccess => Uploaded(commit)
    case error @ DeliveryFailure(message) =>
      logger.error(s"${logMessageCommon(commit)} $message")
      RecoverableError(commit, error)
    case error @ InvalidTriplesFailure(message) =>
      logger.error(s"${logMessageCommon(commit)} $message")
      NonRecoverableError(commit, error: Throwable)
    case error @ InvalidUpdatesFailure(message) =>
      logger.error(s"${logMessageCommon(commit)} $message")
      NonRecoverableError(commit, error: Throwable)
  }

  private def toRecoverableError(commit: CommitEvent): ProcessingRecoverableError => UploadingResult = { error =>
    logger.error(s"${logMessageCommon(commit)} ${error.message}")
    RecoverableError(commit, error)
  }

  private def nonRecoverableFailure(commit: CommitEvent): PartialFunction[Throwable, Interpretation[UploadingResult]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"${logMessageCommon(commit)} failed")
      (NonRecoverableError(commit, exception): UploadingResult).pure[Interpretation]
  }

  private def updateEventLog(uploadingResults: NonEmptyList[UploadingResult]) = {
    for {
      _ <- if (uploadingResults.allUploaded)
            markDone(uploadingResults.head.commit.compoundEventId)
          else if (uploadingResults.haveRecoverableFailure)
            markEventAsRecoverable(uploadingResults.recoverableError)
          else markEventAsNonRecoverable(uploadingResults.nonRecoverableError)
    } yield uploadingResults
  } recoverWith logEventLogUpdateError(uploadingResults)

  private implicit class UploadingResultOps(uploadingResults: NonEmptyList[UploadingResult]) {
    private val resultsByType = uploadingResults.toList.groupBy(_.getClass)

    lazy val allUploaded            = resultsByType.get(classOf[Uploaded]).map(_.size).contains(uploadingResults.size)
    lazy val haveRecoverableFailure = resultsByType.get(classOf[RecoverableError]).map(_.size).exists(_ != 0)
    lazy val recoverableError       = resultsByType.get(classOf[RecoverableError]).flatMap(_.headOption)
    lazy val nonRecoverableError    = resultsByType.get(classOf[NonRecoverableError]).flatMap(_.headOption)
  }

  private def markEventAsRecoverable(maybeUploadingError: Option[UploadingResult]) =
    maybeUploadingError match {
      case Some(RecoverableError(commit, exception)) =>
        markEventFailed(commit.compoundEventId, RecoverableFailure, EventMessage(exception))
      case _ => ME.unit
    }

  private def markEventAsNonRecoverable(maybeUploadingError: Option[UploadingResult]) =
    maybeUploadingError match {
      case Some(NonRecoverableError(commit, exception)) =>
        markEventFailed(commit.compoundEventId, NonRecoverableFailure, EventMessage(exception))
      case _ => ME.unit
    }

  private def logEventLogUpdateError(
      uploadingResults: NonEmptyList[UploadingResult]
  ): PartialFunction[Throwable, Interpretation[NonEmptyList[UploadingResult]]] = {
    case NonFatal(exception) =>
      logger.error(exception)(
        s"${logMessageCommon(uploadingResults.head.commit)} failed to mark as $TriplesStore in the Event Log"
      )
      uploadingResults.pure[Interpretation]
  }

  private def logSummary: ((ElapsedTime, NonEmptyList[UploadingResult])) => Interpretation[Unit] = {
    case (elapsedTime, uploadingResults) =>
      val (uploaded, failed) = uploadingResults.foldLeft(List.empty[Uploaded] -> List.empty[UploadingError]) {
        case ((allUploaded, allFailed), uploaded @ Uploaded(_)) => (allUploaded :+ uploaded) -> allFailed
        case ((allUploaded, allFailed), failed: UploadingError) => allUploaded -> (allFailed :+ failed)
      }
      logger.info(
        s"${logMessageCommon(uploadingResults.head.commit)} processed in ${elapsedTime}ms: " +
          s"${uploadingResults.size} commits, " +
          s"${uploaded.size} commits uploaded, " +
          s"${failed.size} commits failed"
      )
  }

  private def logMessageCommon(event: CommitEvent): String =
    s"Commit Event id: ${event.compoundEventId}, ${event.project.path}"

  private def rollback(commit: CommitEvent): PartialFunction[Throwable, Interpretation[Option[AccessToken]]] = {
    case NonFatal(exception) =>
      markEventNew(commit.compoundEventId)
        .flatMap(_ => ME.raiseError(new Exception("processing failure -> Event rolled back", exception)))
  }

  private sealed trait UploadingResult extends Product with Serializable {
    val commit: CommitEvent
  }
  private sealed trait UploadingError extends UploadingResult {
    val cause: Throwable
  }
  private object UploadingResult {
    case class Uploaded(commit:            CommitEvent) extends UploadingResult
    case class RecoverableError(commit:    CommitEvent, cause: Throwable) extends UploadingError
    case class NonRecoverableError(commit: CommitEvent, cause: Throwable) extends UploadingError
  }
}

private object CommitEventProcessor {
  trait ProcessingRecoverableError extends Exception { val message: String }
}

private object IOCommitEventProcessor {
  import ch.datascience.config.GitLab
  import ch.datascience.control.Throttler
  import com.typesafe.config.{Config, ConfigFactory}

  private[eventprocessing] lazy val eventsProcessingTimesBuilder =
    Histogram
      .build()
      .name("events_processing_times")
      .help("Commit Events processing times")
      .buckets(.1, .5, 1, 5, 10, 50, 100, 500, 1000, 5000)

  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      triplesGenerator:    TriplesGenerator[IO],
      metricsRegistry:     MetricsRegistry[IO],
      gitLabThrottler:     Throttler[IO, GitLab],
      timeRecorder:        SparqlQueryTimeRecorder[IO],
      logger:              Logger[IO],
      config:              Config = ConfigFactory.load()
  )(implicit contextShift: ContextShift[IO],
    executionContext:      ExecutionContext,
    timer:                 Timer[IO]): IO[CommitEventProcessor[IO]] =
    for {
      uploader              <- IOUploader(logger, timeRecorder)
      accessTokenFinder     <- IOAccessTokenFinder(logger)
      triplesCurator        <- IOTriplesCurator(gitLabThrottler, logger, timeRecorder)
      eventStatusUpdater    <- IOEventStatusUpdater(logger)
      eventsProcessingTimes <- metricsRegistry.register[Histogram, Histogram.Builder](eventsProcessingTimesBuilder)
      executionTimeRecorder <- ExecutionTimeRecorder[IO](logger, maybeHistogram = Some(eventsProcessingTimes))
    } yield new CommitEventProcessor(
      accessTokenFinder,
      triplesGenerator,
      triplesCurator,
      uploader,
      eventStatusUpdater,
      new IOEventLogMarkNew(transactor),
      new IOEventLogMarkFailed(transactor),
      logger,
      executionTimeRecorder
    )
}
