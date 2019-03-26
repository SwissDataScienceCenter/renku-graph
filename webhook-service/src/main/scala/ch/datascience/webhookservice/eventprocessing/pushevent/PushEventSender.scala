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

package ch.datascience.webhookservice.eventprocessing.pushevent

import cats.effect.{Clock, ContextShift, IO}
import cats.implicits._
import cats.{Monad, MonadError}
import ch.datascience.graph.model.events.CommitEvent
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder, TokenRepositoryUrlProvider}
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.webhookservice.eventprocessing.PushEvent
import ch.datascience.webhookservice.eventprocessing.commitevent._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class PushEventSender[Interpretation[_]: Monad](
    accessTokenFinder:     AccessTokenFinder[Interpretation],
    commitEventsFinder:    CommitEventsFinder[Interpretation],
    commitEventSender:     CommitEventSender[Interpretation],
    logger:                Logger[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable]) {

  import SendingResult._
  import accessTokenFinder._
  import commitEventSender._
  import commitEventsFinder._
  import executionTimeRecorder._

  def storeCommitsInEventLog(pushEvent: PushEvent): Interpretation[Unit] =
    measureExecutionTime {
      for {
        maybeAccessToken   <- findAccessToken(pushEvent.project.id)
        commitEventsStream <- findCommitEvents(pushEvent, maybeAccessToken)
        sendingResults     <- (commitEventsStream map sendEvent(pushEvent)).sequence
      } yield sendingResults
    } flatMap logSummary(pushEvent) recoverWith loggingError(pushEvent)

  private def sendEvent(pushEvent: PushEvent)(maybeCommitEvent: Interpretation[CommitEvent]) = {
    for {
      commitEvent <- maybeCommitEvent recoverWith fetchErrorLogging
      _           <- send(commitEvent) recoverWith sendErrorLogging(commitEvent)
    } yield Stored: SendingResult
  } recover withLogging(pushEvent)

  private def logSummary(pushEvent: PushEvent): ((ElapsedTime, Stream[SendingResult])) => Interpretation[Unit] = {
    case (elapsedTime, sendingResults) =>
      val groupedByType = sendingResults groupBy identity
      val stored        = groupedByType.get(Stored).map(_.size).getOrElse(0)
      val failed        = groupedByType.get(Failed).map(_.size).getOrElse(0)
      logger.info(
        logMessageFor(
          pushEvent,
          s"${sendingResults.size} Commit Events generated, $stored stored in the Event Log, $failed failed in ${elapsedTime}ms"
        )
      )
  }

  private def loggingError(pushEvent: PushEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(logMessageFor(pushEvent, "storing in event log failed"))
      ME.raiseError(exception)
  }

  private case class CommitEventProcessingException(
      maybeCommitEvent: Option[CommitEvent],
      message:          String,
      cause:            Throwable
  ) extends Exception

  private lazy val fetchErrorLogging: PartialFunction[Throwable, Interpretation[CommitEvent]] = {
    case NonFatal(exception) =>
      ME.raiseError(
        CommitEventProcessingException(
          maybeCommitEvent = None,
          message          = "fetching one of the commit events failed",
          cause            = exception
        )
      )
  }

  private def sendErrorLogging(commitEvent: CommitEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      ME.raiseError(
        CommitEventProcessingException(
          maybeCommitEvent = Some(commitEvent),
          message          = "storing in event log failed",
          cause            = exception
        )
      )
  }

  private def withLogging(pushEvent: PushEvent): PartialFunction[Throwable, SendingResult] = {
    case CommitEventProcessingException(maybeCommitEvent, message, cause) =>
      logger.error(cause)(logMessageFor(pushEvent, message, maybeCommitEvent))
      SendingResult.Failed
    case NonFatal(exception) =>
      ME.raiseError(exception)
      SendingResult.Failed
  }

  private def logMessageFor(
      pushEvent:        PushEvent,
      message:          String,
      maybeCommitEvent: Option[CommitEvent] = None
  ) =
    s"PushEvent commitTo: ${pushEvent.commitTo}, " +
      s"project: ${pushEvent.project.id}" +
      s"${maybeCommitEvent.map(_.id).map(id => s", CommitEvent id: $id").getOrElse("")}" +
      s": $message"

  private sealed trait SendingResult extends Product with Serializable
  private object SendingResult {
    final case object Stored extends SendingResult
    final case object Failed extends SendingResult
  }
}

class IOPushEventSender(
    implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    clock:                     Clock[IO]
) extends PushEventSender[IO](
      new IOAccessTokenFinder(new TokenRepositoryUrlProvider[IO]()),
      new IOCommitEventsFinder(),
      new IOCommitEventSender,
      ApplicationLogger,
      new ExecutionTimeRecorder[IO]
    )
