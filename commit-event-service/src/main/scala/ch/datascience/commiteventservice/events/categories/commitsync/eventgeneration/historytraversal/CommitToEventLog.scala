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

package ch.datascience.commiteventservice.events.categories.commitsync
package eventgeneration.historytraversal

import cats.MonadError
import cats.effect._
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal.EventCreationResult.{Created, Existed, Failed}
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.{CommitEvent, StartCommit}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[eventgeneration] trait CommitToEventLog[Interpretation[_]] {
  def storeCommitsInEventLog(startCommit: StartCommit): Interpretation[Unit]
}

private class CommitToEventLogImpl[Interpretation[_] : MonadError[*[_], Throwable]](
                                                                                     accessTokenFinder: AccessTokenFinder[Interpretation],
                                                                                     commitEventsSource: CommitEventsSourceBuilder[Interpretation],
                                                                                     commitEventSender: CommitEventSender[Interpretation],
                                                                                     eventDetailsFinder: EventDetailsFinder[Interpretation],
                                                                                     logger: Logger[Interpretation],
                                                                                     executionTimeRecorder: ExecutionTimeRecorder[Interpretation],
                                                                                     clock: java.time.Clock = java.time.Clock.systemDefaultZone()
                                                                                   ) extends CommitToEventLog[Interpretation] {

  import IOAccessTokenFinder._
  import accessTokenFinder._
  import commitEventSender._
  import commitEventsSource._
  import executionTimeRecorder._

  def storeCommitsInEventLog(startCommit: StartCommit): Interpretation[Unit] = measureExecutionTime {
    for {
      maybeAccessToken <- findAccessToken(startCommit.project.path)
      commitEventsSource <- buildEventsSource(startCommit, maybeAccessToken, clock)
      sendingResults <-
        commitEventsSource transformEventsWith sendEvent(startCommit) recoverWith eventFindingException
    } yield sendingResults
  } flatMap logSummary(startCommit) recoverWith loggingError(startCommit)

  private def sendEvent(startCommit: StartCommit)(commitEvent: CommitEvent): Interpretation[EventCreationResult] =
    eventDetailsFinder
      .checkIfExists(commitEvent.id, commitEvent.project.id)
      .flatMap {
        case true => Existed.pure[Interpretation].widen[EventCreationResult]
        case false =>
          send(commitEvent)
            .map(_ => Created)
            .widen[EventCreationResult]
            .recover { case NonFatal(exception) =>
              logger.error(exception)(logMessageFor(startCommit, "storing in the event log failed", Some(commitEvent)))
              Failed
            }
      }
      .recover { case NonFatal(exception) =>
        logger.error(exception)(logMessageFor(startCommit, "finding event in the event log failed", Some(commitEvent)))
        Failed
      }

  private lazy val eventFindingException: PartialFunction[Throwable, Interpretation[List[EventCreationResult]]] = {
    case NonFatal(exception) => EventFindingException(exception).raiseError[Interpretation, List[EventCreationResult]]
  }

  private case class EventFindingException(cause: Throwable)
    extends RuntimeException("finding commit events failed", cause)

  private def logSummary(
                          startCommit: StartCommit
                        ): ((ElapsedTime, List[EventCreationResult])) => Interpretation[Unit] = {
    case (_, Nil) => ().pure[Interpretation]
    case (elapsedTime, sendingResults) =>
      val groupedByType = sendingResults groupBy identity
      val created = groupedByType.get(Created).map(_.size).getOrElse(0)
      val existed = groupedByType.get(Existed).map(_.size).getOrElse(0)
      val failed = groupedByType.get(Failed).map(_.size).getOrElse(0)
      logger.info(
        logMessageFor(
          startCommit,
          s"events generation result: $created created, $existed existed, $failed failed in ${elapsedTime}ms"
        )
      )
  }

  private def loggingError(startCommit: StartCommit): PartialFunction[Throwable, Interpretation[Unit]] = {
    case exception@EventFindingException(cause) =>
      logger.error(cause)(logMessageFor(startCommit, exception.getMessage))
      cause.raiseError[Interpretation, Unit]
    case NonFatal(exception) =>
      logger.error(exception)(logMessageFor(startCommit, "converting to commit events failed"))
      exception.raiseError[Interpretation, Unit]
  }

  private def logMessageFor(
                             startCommit: StartCommit,
                             message: String,
                             maybeCommitEvent: Option[CommitEvent] = None
                           ) = {
    val maybeAddedId = maybeCommitEvent.map(event => s", addedId = ${event.id}") getOrElse ""
    s"$categoryName: id = ${startCommit.id}$maybeAddedId, projectId = ${startCommit.project.id}, projectPath = ${startCommit.project.path} -> $message"
  }
}

private[eventgeneration] object CommitToEventLog {
  def apply(
             gitLabThrottler: Throttler[IO, GitLab],
             executionTimeRecorder: ExecutionTimeRecorder[IO],
             logger: Logger[IO]
           )(implicit
             executionContext: ExecutionContext,
             contextShift: ContextShift[IO],
             clock: Clock[IO],
             timer: Timer[IO]
           ): IO[CommitToEventLog[IO]] =
    for {
      eventSender <- CommitEventSender(logger)
      accessTokenFinder <- IOAccessTokenFinder(logger)
      commitEventsSourceBuilder <- CommitEventsSourceBuilder(gitLabThrottler)
      eventDetailsFinder <- EventDetailsFinder(logger)
    } yield new CommitToEventLogImpl[IO](
      accessTokenFinder,
      commitEventsSourceBuilder,
      eventSender,
      eventDetailsFinder,
      logger,
      executionTimeRecorder
    )
}
