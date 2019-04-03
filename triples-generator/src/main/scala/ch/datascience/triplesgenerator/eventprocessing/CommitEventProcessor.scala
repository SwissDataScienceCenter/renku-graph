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
import ch.datascience.dbeventlog.commands.{EventLogMarkDone, EventLogMarkFailed, IOEventLogMarkDone, IOEventLogMarkFailed}
import ch.datascience.dbeventlog.{EventBody, EventLogDB, EventMessage}
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder, TokenRepositoryUrlProvider}
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.triplesgenerator.config.FusekiConfigProvider
import ch.datascience.triplesgenerator.eventprocessing.Commands.GitLabRepoUrlFinder
import ch.datascience.triplesgenerator.eventprocessing.Commit.{CommitWithParent, CommitWithoutParent}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import scala.util.control.NonFatal

class CommitEventProcessor[Interpretation[_]](
    commitEventsDeserialiser: CommitEventsDeserialiser[Interpretation],
    accessTokenFinder:        AccessTokenFinder[Interpretation],
    triplesFinder:            TriplesFinder[Interpretation],
    fusekiConnector:          FusekiConnector[Interpretation],
    eventLogMarkDone:         EventLogMarkDone[Interpretation],
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
  import executionTimeRecorder._
  import fusekiConnector._
  import triplesFinder._

  def apply(eventBody: EventBody): Interpretation[Unit] =
    measureExecutionTime {
      for {
        commits          <- deserialiseToCommitEvents(eventBody)
        maybeAccessToken <- findAccessToken(commits.head.project.id)
        uploadingResults <- (commits map (commit => toTriplesAndUpload(commit, maybeAccessToken))).sequence
      } yield uploadingResults
    } flatMap logSummary recoverWith logEventProcessingError(eventBody)

  private def toTriplesAndUpload(commit:           Commit,
                                 maybeAccessToken: Option[AccessToken]): Interpretation[UploadingResult] = {
    for {
      triples <- generateTriples(commit, maybeAccessToken)
      _       <- upload(triples) recoverWith markEventWithTriplesStoreFailure(commit)
      _       <- markEventDone(commit.commitEventId) recoverWith logError(commit)
    } yield Uploaded(commit, triples.value.size()): UploadingResult
  } recoverWith markEventWithNonRecoverableFailure(commit)

  private def markEventWithTriplesStoreFailure(commit: Commit): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"${logMessageCommon(commit)} failed")
      for {
        _ <- markEventFailed(commit.commitEventId, TriplesStoreFailure, EventMessage(exception)) recoverWith uploadError(
              exception)
        _ <- ME.raiseError[Unit](UploadError(exception))
      } yield ()
  }

  private def uploadError(exception: Throwable): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(_) => ME.raiseError(UploadError(exception))
  }

  private case class UploadError(cause: Throwable) extends Exception(cause)

  private def markEventWithNonRecoverableFailure(
      commit: Commit
  ): PartialFunction[Throwable, Interpretation[UploadingResult]] = {
    case UploadError(_) => ME.pure(Failed(commit))
    case NonFatal(exception) =>
      for {
        _ <- logger.error(exception)(s"${logMessageCommon(commit)} failed")
        _ <- markEventFailed(commit.commitEventId, NonRecoverableFailure, EventMessage(exception))
      } yield Failed(commit)
  }

  private def logSummary: ((ElapsedTime, NonEmptyList[UploadingResult])) => Interpretation[Unit] = {
    case (elapsedTime, uploadingResults) =>
      val (uploaded, failed) = uploadingResults.foldLeft(List.empty[Uploaded] -> List.empty[Failed]) {
        case ((allUploaded, allFailed), uploaded @ Uploaded(_, _)) => (allUploaded :+ uploaded) -> allFailed
        case ((allUploaded, allFailed), failed @ Failed(_))        => allUploaded               -> (allFailed :+ failed)
      }
      logger.info(
        s"${logMessageCommon(uploadingResults.head.commit)} processed in ${elapsedTime}ms: " +
          s"${uploadingResults.size} commits, " +
          s"${uploaded.map(_.numberOfTriples).sum} triples in total, " +
          s"${uploaded.size} commits uploaded, " +
          s"${failed.size} commits failed"
      )
  }

  private def logError(commit: Commit): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"${logMessageCommon(commit)} failed to mark as $TriplesStore in the Event Log")
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

  private sealed trait UploadingResult extends Product with Serializable {
    val commit: Commit
  }
  private object UploadingResult {
    final case class Uploaded(commit: Commit, numberOfTriples: Long) extends UploadingResult
    final case class Failed(commit:   Commit) extends UploadingResult
  }
}

class IOCommitEventProcessor(
    transactor:          DbTransactor[IO, EventLogDB],
    renkuLogTimeout:     FiniteDuration
)(implicit contextShift: ContextShift[IO], executionContext: ExecutionContext, timer: Timer[IO])
    extends CommitEventProcessor[IO](
      new CommitEventsDeserialiser[IO](),
      new IOAccessTokenFinder(new TokenRepositoryUrlProvider[IO]()),
      new IOTriplesFinder(new GitLabRepoUrlFinder[IO](new GitLabUrlProvider[IO]()),
                          new Commands.Renku(renkuLogTimeout)),
      new IOFusekiConnector(new FusekiConfigProvider[IO]()),
      new IOEventLogMarkDone(transactor),
      new IOEventLogMarkFailed(transactor),
      ApplicationLogger,
      new ExecutionTimeRecorder[IO]
    )
