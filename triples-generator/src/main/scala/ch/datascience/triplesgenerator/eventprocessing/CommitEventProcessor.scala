/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventStatus._
import ch.datascience.dbeventlog.commands._
import ch.datascience.dbeventlog.{EventBody, EventLogDB, EventMessage}
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.triplesgenerator.eventprocessing.Commit.{CommitWithParent, CommitWithoutParent}
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.{IOTriplesCurator, TriplesCurator}
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator
import ch.datascience.triplesgenerator.eventprocessing.triplesuploading.TriplesUploadResult._
import ch.datascience.triplesgenerator.eventprocessing.triplesuploading.{IOUploader, TriplesUploadResult, Uploader}
import io.chrisdavenport.log4cats.Logger
import io.prometheus.client.Histogram

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class CommitEventProcessor[Interpretation[_]](
    commitEventsDeserialiser: CommitEventsDeserialiser[Interpretation],
    accessTokenFinder:        AccessTokenFinder[Interpretation],
    triplesGenerator:         TriplesGenerator[Interpretation],
    triplesCurator:           TriplesCurator[Interpretation],
    uploader:                 Uploader[Interpretation],
    eventLogMarkDone:         EventLogMarkDone[Interpretation],
    eventLogMarkNew:          EventLogMarkNew[Interpretation],
    eventLogMarkFailed:       EventLogMarkFailed[Interpretation],
    logger:                   Logger[Interpretation],
    executionTimeRecorder:    ExecutionTimeRecorder[Interpretation]
)(implicit ME:                MonadError[Interpretation, Throwable])
    extends EventProcessor[Interpretation] {

  import UploadingResult._
  import accessTokenFinder._
  import commitEventsDeserialiser._
  import eventLogMarkDone._
  import eventLogMarkFailed._
  import eventLogMarkNew._
  import executionTimeRecorder._
  import triplesCurator._
  import triplesGenerator._
  import uploader._

  def apply(eventBody: EventBody): Interpretation[Unit] =
    measureExecutionTime {
      for {
        commits          <- deserialiseToCommitEvents(eventBody)
        maybeAccessToken <- findAccessToken(commits.head.project.id) recoverWith rollback(commits.head)
        uploadingResults <- allToTriplesAndUpload(commits, maybeAccessToken)
      } yield uploadingResults
    } flatMap logSummary recoverWith logEventProcessingError(eventBody)

  private def allToTriplesAndUpload(
      commits:          NonEmptyList[Commit],
      maybeAccessToken: Option[AccessToken]
  ): Interpretation[NonEmptyList[UploadingResult]] =
    commits
      .map(toTriplesAndUpload(_, maybeAccessToken))
      .sequence
      .flatMap(updateEventLog)

  private def toTriplesAndUpload(commit:           Commit,
                                 maybeAccessToken: Option[AccessToken]): Interpretation[UploadingResult] = {
    for {
      rawTriples     <- generateTriples(commit, maybeAccessToken)
      curatedTriples <- curate(rawTriples)
      result         <- upload(curatedTriples) map toUploadingResult(commit)
    } yield result
  } recoverWith nonRecoverableFailure(commit)

  private def toUploadingResult(commit: Commit): TriplesUploadResult => UploadingResult = {
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

  private def nonRecoverableFailure(commit: Commit): PartialFunction[Throwable, Interpretation[UploadingResult]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"${logMessageCommon(commit)} failed")
      ME.pure(NonRecoverableError(commit, exception): UploadingResult)
  }

  private def updateEventLog(uploadingResults: NonEmptyList[UploadingResult]) = {
    for {
      _ <- if (uploadingResults.allUploaded)
            markEventDone(uploadingResults.head.commit.commitEventId)
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
        markEventFailed(commit.commitEventId, TriplesStoreFailure, EventMessage(exception))
      case _ => ME.unit
    }

  private def markEventAsNonRecoverable(maybeUploadingError: Option[UploadingResult]) =
    maybeUploadingError match {
      case Some(NonRecoverableError(commit, exception)) =>
        markEventFailed(commit.commitEventId, NonRecoverableFailure, EventMessage(exception))
      case _ => ME.unit
    }

  private def logEventLogUpdateError(
      uploadingResults: NonEmptyList[UploadingResult]
  ): PartialFunction[Throwable, Interpretation[NonEmptyList[UploadingResult]]] = {
    case NonFatal(exception) =>
      logger.error(exception)(
        s"${logMessageCommon(uploadingResults.head.commit)} failed to mark as $TriplesStore in the Event Log"
      )
      ME.pure(uploadingResults)
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

  private lazy val logMessageCommon: Commit => String = {
    case CommitWithoutParent(id, project) =>
      s"Commit Event id: $id, project: ${project.id} ${project.path}"
    case CommitWithParent(id, parentId, project) =>
      s"Commit Event id: $id, project: ${project.id} ${project.path}, parentId: $parentId"
  }

  private def logEventProcessingError(eventBody: EventBody): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) => logger.error(exception)(s"Commit Event processing failure: $eventBody")
  }

  private def rollback(commit: Commit): PartialFunction[Throwable, Interpretation[Option[AccessToken]]] = {
    case NonFatal(exception) =>
      markEventNew(commit.commitEventId)
        .flatMap(_ => ME.raiseError(new Exception("processing failure -> Event rolled back", exception)))
  }

  private sealed trait UploadingResult extends Product with Serializable {
    val commit: Commit
  }
  private sealed trait UploadingError extends UploadingResult {
    val cause: Throwable
  }
  private object UploadingResult {
    case class Uploaded(commit:            Commit) extends UploadingResult
    case class RecoverableError(commit:    Commit, cause: Throwable) extends UploadingError
    case class NonRecoverableError(commit: Commit, cause: Throwable) extends UploadingError
  }
}

object IOCommitEventProcessor {

  import ch.datascience.metrics.MetricsRegistry

  private[triplesgenerator] lazy val eventsProcessingTimes: Histogram = MetricsRegistry.register {
    Histogram
      .build()
      .name("events_processing_times")
      .help("Commit Events processing times")
      .register(_)
  }

  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      triplesGenerator:    TriplesGenerator[IO]
  )(implicit contextShift: ContextShift[IO],
    executionContext:      ExecutionContext,
    timer:                 Timer[IO]): IO[CommitEventProcessor[IO]] =
    for {
      uploader          <- IOUploader(ApplicationLogger)
      accessTokenFinder <- IOAccessTokenFinder(ApplicationLogger)
      executionTimeRecorder <- ExecutionTimeRecorder[IO](ApplicationLogger,
                                                         maybeHistogram = Some(eventsProcessingTimes))
    } yield new CommitEventProcessor[IO](
      new CommitEventsDeserialiser[IO](),
      accessTokenFinder,
      triplesGenerator,
      IOTriplesCurator(),
      uploader,
      new IOEventLogMarkDone(transactor),
      new IOEventLogMarkNew(transactor),
      new IOEventLogMarkFailed(transactor),
      ApplicationLogger,
      executionTimeRecorder
    )
}
