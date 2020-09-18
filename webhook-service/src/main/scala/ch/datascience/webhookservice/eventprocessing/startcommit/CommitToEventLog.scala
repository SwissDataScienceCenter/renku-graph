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

package ch.datascience.webhookservice.eventprocessing.startcommit

import cats.effect._
import cats.syntax.all._
import cats.{Monad, MonadError}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.webhookservice.eventprocessing.commitevent._
import ch.datascience.webhookservice.eventprocessing.{CommitEvent, StartCommit}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class CommitToEventLog[Interpretation[_]: Monad](
    accessTokenFinder:     AccessTokenFinder[Interpretation],
    commitEventsSource:    CommitEventsSourceBuilder[Interpretation],
    commitEventSender:     CommitEventSender[Interpretation],
    logger:                Logger[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
    clock:                 java.time.Clock = java.time.Clock.systemDefaultZone()
)(implicit ME:             MonadError[Interpretation, Throwable]) {

  import CommitEventSender.EventSendingResult._
  import IOAccessTokenFinder._
  import SendingResult._
  import accessTokenFinder._
  import commitEventSender._
  import commitEventsSource._
  import executionTimeRecorder._

  def storeCommitsInEventLog(startCommit: StartCommit): Interpretation[Unit] =
    measureExecutionTime {
      for {
        maybeAccessToken   <- findAccessToken(startCommit.project.id)
        commitEventsSource <- buildEventsSource(startCommit, maybeAccessToken, clock)
        sendingResults <-
          commitEventsSource transformEventsWith sendEvent(startCommit) recoverWith eventFindingException
      } yield sendingResults
    } flatMap logSummary(startCommit) recoverWith loggingError(startCommit)

  private def sendEvent(startCommit: StartCommit)(commitEvent: CommitEvent): Interpretation[SendingResult] =
    send(commitEvent)
      .map {
        case EventCreated => Created
        case EventExisted => Existed
      }
      .recover { case NonFatal(exception) =>
        logger.error(exception)(logMessageFor(startCommit, "storing in the event log failed", Some(commitEvent)))
        Failed
      }

  private lazy val eventFindingException: PartialFunction[Throwable, Interpretation[List[SendingResult]]] = {
    case NonFatal(exception) => EventFindingException(exception).raiseError[Interpretation, List[SendingResult]]
  }

  private case class EventFindingException(cause: Throwable)
      extends RuntimeException("finding commit events failed", cause)

  private def logSummary(startCommit: StartCommit): ((ElapsedTime, List[SendingResult])) => Interpretation[Unit] = {
    case (_, Nil) => ME.unit
    case (elapsedTime, sendingResults) =>
      val groupedByType = sendingResults groupBy identity
      val created       = groupedByType.get(Created).map(_.size).getOrElse(0)
      val existed       = groupedByType.get(Existed).map(_.size).getOrElse(0)
      val failed        = groupedByType.get(Failed).map(_.size).getOrElse(0)
      logger.info(
        logMessageFor(
          startCommit,
          s"${sendingResults.size} Commit Events generated: $created created, $existed existed, $failed failed in ${elapsedTime}ms"
        )
      )
  }

  private def loggingError(startCommit: StartCommit): PartialFunction[Throwable, Interpretation[Unit]] = {
    case exception @ EventFindingException(cause) =>
      logger.error(cause)(logMessageFor(startCommit, exception.getMessage))
      ME.raiseError(cause)
    case NonFatal(exception) =>
      logger.error(exception)(logMessageFor(startCommit, "converting to commit events failed"))
      ME.raiseError(exception)
  }

  private def logMessageFor(
      startCommit:      StartCommit,
      message:          String,
      maybeCommitEvent: Option[CommitEvent] = None
  ) =
    s"Start Commit id: ${startCommit.id}, " +
      s"project: ${startCommit.project.id}" +
      s"${maybeCommitEvent.map(_.id).map(id => s", CommitEvent id: $id").getOrElse("")}" +
      s": $message"
}

object IOCommitToEventLog {
  def apply(
      gitLabThrottler:       Throttler[IO, GitLab],
      executionTimeRecorder: ExecutionTimeRecorder[IO],
      logger:                Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      clock:            Clock[IO],
      timer:            Timer[IO]
  ): IO[CommitToEventLog[IO]] =
    for {
      eventSender               <- IOCommitEventSender(logger)
      accessTokenFinder         <- IOAccessTokenFinder(logger)
      commitEventsSourceBuilder <- IOCommitEventsSourceBuilder(gitLabThrottler)
    } yield new CommitToEventLog[IO](
      accessTokenFinder,
      commitEventsSourceBuilder,
      eventSender,
      logger,
      executionTimeRecorder
    )
}
