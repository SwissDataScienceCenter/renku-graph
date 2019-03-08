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
import ch.datascience.graph.model.events.{CommitEvent, CommitId}
import ch.datascience.http.client.AccessToken
import ch.datascience.webhookservice.config.GitLabConfigProvider
import ch.datascience.webhookservice.eventprocessing.PushEvent

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

private class CommitEventsFinder[Interpretation[_]](
    commitInfoFinder: CommitInfoFinder[Interpretation]
)(implicit ME:        MonadError[Interpretation, Throwable]) {

  import commitInfoFinder._

  import Stream._
  type CommitEventsStream = Interpretation[Stream[Interpretation[CommitEvent]]]

  def findCommitEvents(pushEvent: PushEvent, maybeAccessToken: Option[AccessToken]): CommitEventsStream =
    stream(List(pushEvent.commitTo), pushEvent, maybeAccessToken)

  private def stream(commitIds:        List[CommitId],
                     pushEvent:        PushEvent,
                     maybeAccessToken: Option[AccessToken]): CommitEventsStream =
    commitIds match {
      case Nil => ME.pure(Stream.empty[Interpretation[CommitEvent]])
      case commitId +: commitIdsStillToProcess =>
        nextElement(commitId, commitIdsStillToProcess, pushEvent, maybeAccessToken)
    }

  private def nextElement(commitId:                CommitId,
                          commitIdsStillToProcess: List[CommitId],
                          pushEvent:               PushEvent,
                          maybeAccessToken:        Option[AccessToken]) = {
    for {
      commitEvent <- findCommitEvent(commitId, pushEvent, maybeAccessToken)
      commitIdsToProcess = findCommitIdsToProcess(commitIdsStillToProcess,
                                                  commitEvent.parents,
                                                  pushEvent.maybeCommitFrom)
      nextCommitEvent <- stream(commitIdsToProcess, pushEvent, maybeAccessToken)
    } yield ME.pure(commitEvent) #:: nextCommitEvent
  } recoverWith elementForFailure(commitIdsStillToProcess, pushEvent, maybeAccessToken)

  private def findCommitIdsToProcess(commitsToProcess:    List[CommitId],
                                     parentCommits:       List[CommitId],
                                     maybeEarliestCommit: Option[CommitId]): List[CommitId] =
    commitsToProcess ++ parentCommits takeWhile (commitId => !maybeEarliestCommit.contains(commitId))

  private def elementForFailure(
      commitIdsToProcess: List[CommitId],
      pushEvent:          PushEvent,
      maybeAccessToken:   Option[AccessToken]): PartialFunction[Throwable, CommitEventsStream] = {
    case NonFatal(exception) =>
      stream(commitIdsToProcess, pushEvent, maybeAccessToken) map (ME.raiseError[CommitEvent](exception) #:: _)
  }

  private def findCommitEvent(commitId:         CommitId,
                              pushEvent:        PushEvent,
                              maybeAccessToken: Option[AccessToken]): Interpretation[CommitEvent] =
    for {
      commitInfo <- findCommitInfo(pushEvent.project.id, commitId, maybeAccessToken)
    } yield merge(commitInfo, pushEvent)

  private def merge(commitInfo: CommitInfo, pushEvent: PushEvent): CommitEvent =
    CommitEvent(
      id            = commitInfo.id,
      message       = commitInfo.message,
      committedDate = commitInfo.committedDate,
      pushUser      = pushEvent.pushUser,
      author        = commitInfo.author,
      committer     = commitInfo.committer,
      parents       = commitInfo.parents,
      project       = pushEvent.project
    )
}

private class IOCommitEventsFinder(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO])
    extends CommitEventsFinder[IO](
      new IOCommitInfoFinder(new GitLabConfigProvider())
    )
