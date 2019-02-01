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
import cats.effect.IO
import cats.implicits._
import ch.datascience.graph.events.{CommitEvent, CommitId}
import ch.datascience.webhookservice.eventprocessing.CommitEventsOrigin
import javax.inject.{Inject, Singleton}

import scala.language.higherKinds
import scala.util.control.NonFatal

class CommitEventsFinder[Interpretation[_]](
    commitInfoFinder: CommitInfoFinder[Interpretation]
)(implicit ME:        MonadError[Interpretation, Throwable]) {

  import commitInfoFinder._

  import Stream._
  type CommitEventsStream = Interpretation[Stream[Interpretation[CommitEvent]]]

  def findCommitEvents(commitEventsOrigin: CommitEventsOrigin): CommitEventsStream =
    stream(List(commitEventsOrigin.commitTo), commitEventsOrigin)

  private def stream(commitIds: List[CommitId], commitEventsOrigin: CommitEventsOrigin): CommitEventsStream =
    commitIds match {
      case Nil                                 => ME.pure(Stream.empty[Interpretation[CommitEvent]])
      case commitId +: commitIdsStillToProcess => nextElement(commitId, commitIdsStillToProcess, commitEventsOrigin)
    }

  private def nextElement(commitId:                CommitId,
                          commitIdsStillToProcess: List[CommitId],
                          commitEventsOrigin:      CommitEventsOrigin) = {
    for {
      commitEvent <- findCommitEvent(commitId, commitEventsOrigin)
      commitIdsToProcess = findCommitIdsToProcess(commitIdsStillToProcess,
                                                  commitEvent.parents,
                                                  commitEventsOrigin.maybeCommitFrom)
      nextCommitEvent <- stream(commitIdsToProcess, commitEventsOrigin)
    } yield ME.pure(commitEvent) #:: nextCommitEvent
  } recoverWith elementForFailure(commitIdsStillToProcess, commitEventsOrigin)

  private def findCommitIdsToProcess(commitsToProcess:    List[CommitId],
                                     parentCommits:       List[CommitId],
                                     maybeEarliestCommit: Option[CommitId]): List[CommitId] =
    (commitsToProcess ++ parentCommits).takeWhile(commitId => !maybeEarliestCommit.contains(commitId))

  private def elementForFailure(
      commitIdsToProcess: List[CommitId],
      commitEventsOrigin: CommitEventsOrigin): PartialFunction[Throwable, CommitEventsStream] = {
    case NonFatal(exception) =>
      stream(commitIdsToProcess, commitEventsOrigin) map (ME.raiseError[CommitEvent](exception) #:: _)
  }

  private def findCommitEvent(commitId: CommitId, commitEventsOrigin: CommitEventsOrigin): Interpretation[CommitEvent] =
    for {
      commitInfo <- findCommitInfo(commitEventsOrigin.project.id, commitId)
    } yield merge(commitInfo, commitEventsOrigin)

  private def merge(commitInfo: CommitInfo, commitEventsOrigin: CommitEventsOrigin): CommitEvent =
    CommitEvent(
      id              = commitInfo.id,
      message         = commitInfo.message,
      committedDate   = commitInfo.committedDate,
      pushUser        = commitEventsOrigin.pushUser,
      author          = commitInfo.author,
      committer       = commitInfo.committer,
      parents         = commitInfo.parents,
      project         = commitEventsOrigin.project,
      hookAccessToken = commitEventsOrigin.hookAccessToken
    )
}

@Singleton
class IOCommitEventsFinder @Inject()(
    commitInfoFinder: IOCommitInfoFinder
) extends CommitEventsFinder[IO](commitInfoFinder)
