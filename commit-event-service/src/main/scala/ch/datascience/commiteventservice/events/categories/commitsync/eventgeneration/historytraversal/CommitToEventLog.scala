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

import cats.effect._
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEvent.{NewCommitEvent, SkippedCommitEvent}
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.{CommitEvent, CommitInfo, Project, StartCommit}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.events.{BatchDate, CommitId}
import ch.datascience.http.client.AccessToken
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[eventgeneration] trait CommitToEventLog[Interpretation[_]] {
  def storeCommitsInEventLog(startCommit:      StartCommit,
                             maybeAccessToken: Option[AccessToken]
  ): Interpretation[UpdateResult]
}

private class CommitToEventLogImpl[Interpretation[_]: MonadThrow](
    commitEventSender:  CommitEventSender[Interpretation],
    eventDetailsFinder: EventDetailsFinder[Interpretation],
    commitInfoFinder:   CommitInfoFinder[Interpretation],
    logger:             Logger[Interpretation],
    clock:              java.time.Clock = java.time.Clock.systemUTC()
) extends CommitToEventLog[Interpretation] {

  import commitEventSender._

  private val DontCareCommitId = CommitId("0000000000000000000000000000000000000000")

  def storeCommitsInEventLog(startCommit:      StartCommit,
                             maybeAccessToken: Option[AccessToken]
  ): Interpretation[UpdateResult] = {
    maybeCommitEvent(startCommit.project, startCommit.id, BatchDate(clock), maybeAccessToken) flatMap {
      case None              => Skipped.pure[Interpretation].widen[UpdateResult]
      case Some(commitEvent) => sendEvent(startCommit)(commitEvent)
    } recoverWith eventFindingException
  } recoverWith loggingError(startCommit)

  private def sendEvent(startCommit: StartCommit)(commitEvent: CommitEvent): Interpretation[UpdateResult] =
    eventDetailsFinder
      .checkIfExists(commitEvent.project.id, commitEvent.id)
      .flatMap {
        case true => Existed.pure[Interpretation].widen[UpdateResult]
        case false =>
          send(commitEvent)
            .map(_ => Created)
            .widen[UpdateResult]
            .recover { case NonFatal(exception) =>
              Failed(logMessageFor(startCommit, "storing in the event log failed", Some(commitEvent)), exception)
            }
      }
      .recover { case NonFatal(exception) =>
        Failed(logMessageFor(startCommit, "checking if event exists in the event log failed", Some(commitEvent)),
               exception
        )
      }

  private def maybeCommitEvent(project:          Project,
                               commitId:         CommitId,
                               batchDate:        BatchDate,
                               maybeAccessToken: Option[AccessToken]
  ): Interpretation[Option[CommitEvent]] =
    if (commitId == DontCareCommitId) Option.empty[CommitEvent].pure[Interpretation]
    else findCommitEvent(project, commitId, batchDate, maybeAccessToken).map(Option.apply)

  private def findCommitEvent(project:          Project,
                              commitId:         CommitId,
                              batchDate:        BatchDate,
                              maybeAccessToken: Option[AccessToken]
  ): Interpretation[CommitEvent] =
    commitInfoFinder.findCommitInfo(project.id, commitId, maybeAccessToken) map toCommitEvent(project, batchDate)

  private def toCommitEvent(project: Project, batchDate: BatchDate)(commitInfo: CommitInfo) =
    commitInfo.message.value match {
      case message if message contains "renku migrate" =>
        SkippedCommitEvent(
          id = commitInfo.id,
          message = commitInfo.message,
          committedDate = commitInfo.committedDate,
          author = commitInfo.author,
          committer = commitInfo.committer,
          parents = commitInfo.parents.filterNot(_ == DontCareCommitId),
          project = project,
          batchDate = batchDate
        )
      case _ =>
        NewCommitEvent(
          id = commitInfo.id,
          message = commitInfo.message,
          committedDate = commitInfo.committedDate,
          author = commitInfo.author,
          committer = commitInfo.committer,
          parents = commitInfo.parents.filterNot(_ == DontCareCommitId),
          project = project,
          batchDate = batchDate
        )
    }

  private lazy val eventFindingException: PartialFunction[Throwable, Interpretation[UpdateResult]] = {
    case NonFatal(exception) => EventFindingException(exception).raiseError[Interpretation, UpdateResult]
  }

  private case class EventFindingException(cause: Throwable)
      extends RuntimeException("finding commit events failed", cause)

  private def loggingError(
      startCommit: StartCommit
  ): PartialFunction[Throwable, Interpretation[UpdateResult]] = { case exception @ EventFindingException(cause) =>
    logger.error(cause)(logMessageFor(startCommit, exception.getMessage)) >>
      cause.raiseError[Interpretation, UpdateResult]
  }

  private def logMessageFor(
      startCommit:      StartCommit,
      message:          String,
      maybeCommitEvent: Option[CommitEvent] = None
  ) = {
    val maybeAddedId = maybeCommitEvent.map(event => s", addedId = ${event.id}") getOrElse ""
    s"$categoryName: id = ${startCommit.id}$maybeAddedId, projectId = ${startCommit.project.id}, projectPath = ${startCommit.project.path} -> $message"
  }
}

private[eventgeneration] object CommitToEventLog {
  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      clock:            Clock[IO],
      timer:            Timer[IO]
  ): IO[CommitToEventLog[IO]] =
    for {
      eventSender        <- CommitEventSender(logger)
      eventDetailsFinder <- EventDetailsFinder(logger)
      commitInfoFinder   <- CommitInfoFinder(gitLabThrottler, logger)
    } yield new CommitToEventLogImpl[IO](
      eventSender,
      eventDetailsFinder,
      commitInfoFinder,
      logger
    )
}
