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

package ch.datascience.webhookservice.eventprocessing.startcommit

import cats.effect._
import cats.implicits._
import cats.{Monad, MonadError}
import ch.datascience.control.Throttler
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.events.CommitEvent
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder, TokenRepositoryUrl}
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.webhookservice.config.GitLab
import ch.datascience.webhookservice.eventprocessing.StartCommit
import ch.datascience.webhookservice.eventprocessing.commitevent._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class CommitToEventLog[Interpretation[_]: Monad](
    accessTokenFinder:     AccessTokenFinder[Interpretation],
    commitEventsSource:    CommitEventsSourceBuilder[Interpretation],
    commitEventSender:     CommitEventSender[Interpretation],
    logger:                Logger[Interpretation],
    executionTimeRecorder: ExecutionTimeRecorder[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable]) {

  import CommitToEventLog.SendingResult
  import CommitToEventLog.SendingResult._
  import accessTokenFinder._
  import commitEventSender._
  import commitEventsSource._
  import executionTimeRecorder._

  def storeCommitsInEventLog(startCommit: StartCommit): Interpretation[Unit] =
    measureExecutionTime {
      for {
        maybeAccessToken   <- findAccessToken(startCommit.project.id)
        commitEventsSource <- buildEventsSource(startCommit, maybeAccessToken)
        sendingResults     <- commitEventsSource transformEventsWith sendEvent(startCommit) recoverWith findingEventException
      } yield sendingResults
    } flatMap logSummary(startCommit) recoverWith loggingError(startCommit)

  private def sendEvent(startCommit: StartCommit)(commitEvent: CommitEvent): Interpretation[SendingResult] =
    send(commitEvent)
      .map(_ => Stored: SendingResult)
      .recover(sendErrorLogging(startCommit, commitEvent))

  private def sendErrorLogging(startCommit: StartCommit,
                               commitEvent: CommitEvent): PartialFunction[Throwable, SendingResult] = {
    case NonFatal(exception) =>
      logger.error(exception)(logMessageFor(startCommit, "storing in the event log failed", Some(commitEvent)))
      SendingResult.Failed
  }

  private lazy val findingEventException: PartialFunction[Throwable, Interpretation[List[SendingResult]]] = {
    case NonFatal(exception) => ME.raiseError(EventFindingException(exception))
  }

  private case class EventFindingException(cause: Throwable)
      extends RuntimeException("finding commit events failed", cause)

  private def logSummary(startCommit: StartCommit): ((ElapsedTime, List[SendingResult])) => Interpretation[Unit] = {
    case (elapsedTime, sendingResults) =>
      val groupedByType = sendingResults groupBy identity
      val stored        = groupedByType.get(Stored).map(_.size).getOrElse(0)
      val failed        = groupedByType.get(Failed).map(_.size).getOrElse(0)
      logger.info(
        logMessageFor(
          startCommit,
          s"${sendingResults.size} Commit Events generated, $stored stored in the Event Log, $failed failed in ${elapsedTime}ms"
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

private object CommitToEventLog {
  sealed trait SendingResult extends Product with Serializable
  object SendingResult {
    final case object Stored extends SendingResult
    final case object Failed extends SendingResult
  }
}

class IOCommitToEventLog(
    transactor:              DbTransactor[IO, EventLogDB],
    tokenRepositoryUrl:      TokenRepositoryUrl,
    gitLabUrl:               GitLabUrl,
    gitLabThrottler:         Throttler[IO, GitLab]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], clock: Clock[IO], timer: Timer[IO])
    extends CommitToEventLog[IO](
      new IOAccessTokenFinder(tokenRepositoryUrl, ApplicationLogger),
      new IOCommitEventsSourceBuilder(transactor, gitLabUrl, gitLabThrottler),
      new IOCommitEventSender(transactor),
      ApplicationLogger,
      new ExecutionTimeRecorder[IO]
    )
