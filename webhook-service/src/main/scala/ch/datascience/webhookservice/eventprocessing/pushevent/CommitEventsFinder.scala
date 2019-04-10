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

import cats.MonadError
import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.control.Throttler
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.dbeventlog.commands._
import ch.datascience.graph.gitlab.GitLab
import ch.datascience.graph.model.events.{CommitEvent, CommitId}
import ch.datascience.http.client.AccessToken
import ch.datascience.webhookservice.config.GitLabConfigProvider
import ch.datascience.webhookservice.eventprocessing.PushEvent

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

private class CommitEventsFinder[Interpretation[_]](
    commitInfoFinder:        CommitInfoFinder[Interpretation],
    eventLogLatestEvent:     EventLogLatestEvent[Interpretation],
    eventLogVerifyExistence: EventLogVerifyExistence[Interpretation]
)(implicit ME:               MonadError[Interpretation, Throwable]) {

  import commitInfoFinder._
  import eventLogLatestEvent._
  import eventLogVerifyExistence._

  import Stream._
  type EventsStream = Stream[Interpretation[CommitEvent]]
  private val DontCareCommitId = CommitId("0000000000000000000000000000000000000000")

  def findCommitEvents(pushEvent: PushEvent, maybeAccessToken: Option[AccessToken]): Interpretation[EventsStream] =
    for {
      maybeYoungestEventInLog <- findYoungestEventInLog(pushEvent.project.id)
      stream                  <- new StreamBuilder(pushEvent, maybeYoungestEventInLog, maybeAccessToken).build
    } yield stream

  private class StreamBuilder(pushEvent:               PushEvent,
                              maybeYoungestEventInLog: Option[CommitId],
                              maybeAccessToken:        Option[AccessToken]) {

    def build: Interpretation[EventsStream] = maybeYoungestEventInLog match {
      case Some(pushEvent.commitTo) => ME.pure(Stream.empty)
      case _                        => stream(List(pushEvent.commitTo))
    }

    private def stream(commitIds: List[CommitId]): Interpretation[EventsStream] =
      commitIds match {
        case Nil                               => ME.pure(Stream.empty)
        case DontCareCommitId +: leftToProcess => stream(leftToProcess)
        case commitId +: leftToProcess         => nextElement(commitId, leftToProcess)
      }

    private def nextElement(commitId: CommitId, leftToProcess: List[CommitId]) = {
      for {
        commitEvent     <- findCommitEvent(commitId)
        nextToProcess   <- findNextToProcess(leftToProcess, commitEvent.parents)
        nextCommitEvent <- stream(nextToProcess)
      } yield ME.pure(commitEvent) #:: nextCommitEvent
    } recoverWith elementForFailure(leftToProcess)

    private def findCommitEvent(commitId: CommitId): Interpretation[CommitEvent] =
      findCommitInfo(pushEvent.project.id, commitId, maybeAccessToken)
        .map(commitInfo => merge(commitInfo, pushEvent))

    private def merge(commitInfo: CommitInfo, pushEvent: PushEvent): CommitEvent =
      CommitEvent(
        id            = commitInfo.id,
        message       = commitInfo.message,
        committedDate = commitInfo.committedDate,
        pushUser      = pushEvent.pushUser,
        author        = commitInfo.author,
        committer     = commitInfo.committer,
        parents       = commitInfo.parents.filterNot(_ == DontCareCommitId),
        project       = pushEvent.project
      )

    private def findNextToProcess(leftToProcess: List[CommitId],
                                  parents:       List[CommitId]): Interpretation[List[CommitId]] =
      parents match {
        case Nil => ME.pure(leftToProcess)
        case singleParent +: Nil =>
          if (maybeYoungestEventInLog contains singleParent) ME.pure(leftToProcess)
          else ME.pure(leftToProcess :+ singleParent)
        case manyParents =>
          filterNotExistingInLog(manyParents, pushEvent.project.id)
            .map(leftToProcess ++ _)
            .recoverWith(eventLogException)
      }

    private def elementForFailure(
        leftToProcess: List[CommitId]
    ): PartialFunction[Throwable, Interpretation[EventsStream]] = {
      case EventLogException(exception) =>
        ME.raiseError(exception)
      case NonFatal(exception) =>
        stream(leftToProcess) map (ME.raiseError[CommitEvent](exception) #:: _)
    }

    private final case class EventLogException(cause: Throwable) extends Exception(cause)

    private lazy val eventLogException: PartialFunction[Throwable, Interpretation[List[CommitId]]] = {
      case NonFatal(exception) => ME.raiseError(EventLogException(exception))
    }
  }
}

private class IOCommitEventsFinder(
    transactor:              DbTransactor[IO, EventLogDB],
    gitLabThrottler:         Throttler[IO, GitLab]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO])
    extends CommitEventsFinder[IO](
      new IOCommitInfoFinder(new GitLabConfigProvider(), gitLabThrottler),
      new IOEventLogLatestEvent(transactor),
      new IOEventLogVerifyExistence(transactor)
    )
