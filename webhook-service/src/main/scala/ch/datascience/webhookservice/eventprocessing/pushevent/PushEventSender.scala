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

import cats.effect._
import cats.implicits._
import cats.{Monad, MonadError}
import ch.datascience.control.Throttler
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.graph.gitlab.GitLab
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
    commitEventsSource:    CommitEventsSourceBuilder[Interpretation],
    commitEventSender:     CommitEventSender[Interpretation],
    logger:                Logger[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable]) {

  import PushEventSender.SendingResult
  import PushEventSender.SendingResult._
  import accessTokenFinder._
  import commitEventSender._
  import commitEventsSource._
  import executionTimeRecorder._

  def storeCommitsInEventLog(pushEvent: PushEvent): Interpretation[Unit] =
    measureExecutionTime {
      for {
        maybeAccessToken   <- findAccessToken(pushEvent.project.id)
        commitEventsSource <- buildEventsSource(pushEvent, maybeAccessToken)
        sendingResults     <- commitEventsSource transformEventsWith sendEvent(pushEvent) recoverWith findingEventException
      } yield sendingResults
    } flatMap logSummary(pushEvent) recoverWith loggingError(pushEvent)

  private def sendEvent(pushEvent: PushEvent)(commitEvent: CommitEvent): Interpretation[SendingResult] =
    send(commitEvent)
      .map(_ => Stored: SendingResult)
      .recover(sendErrorLogging(pushEvent, commitEvent))

  private def sendErrorLogging(pushEvent:   PushEvent,
                               commitEvent: CommitEvent): PartialFunction[Throwable, SendingResult] = {
    case NonFatal(exception) =>
      logger.error(exception)(logMessageFor(pushEvent, "storing in the event log failed", Some(commitEvent)))
      SendingResult.Failed
  }

  private lazy val findingEventException: PartialFunction[Throwable, Interpretation[List[SendingResult]]] = {
    case NonFatal(exception) => ME.raiseError(EventFindingException(exception))
  }

  private case class EventFindingException(cause: Throwable)
      extends RuntimeException("finding commit events failed", cause)

  private def logSummary(pushEvent: PushEvent): ((ElapsedTime, List[SendingResult])) => Interpretation[Unit] = {
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
    case exception @ EventFindingException(cause) =>
      logger.error(cause)(logMessageFor(pushEvent, exception.getMessage))
      ME.raiseError(cause)
    case NonFatal(exception) =>
      logger.error(exception)(logMessageFor(pushEvent, "converting to commit events failed"))
      ME.raiseError(exception)
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
}

private object PushEventSender {
  sealed trait SendingResult extends Product with Serializable
  object SendingResult {
    final case object Stored extends SendingResult
    final case object Failed extends SendingResult
  }
}

class IOPushEventSender(
    transactor:              DbTransactor[IO, EventLogDB],
    gitLabThrottler:         Throttler[IO, GitLab]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], clock: Clock[IO], timer: Timer[IO])
    extends PushEventSender[IO](
      new IOAccessTokenFinder(new TokenRepositoryUrlProvider[IO](), ApplicationLogger),
      new IOCommitEventsSourceBuilder(transactor, gitLabThrottler),
      new IOCommitEventSender(transactor),
      ApplicationLogger,
      new ExecutionTimeRecorder[IO]
    )
