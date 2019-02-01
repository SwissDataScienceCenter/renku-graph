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
import ch.datascience.webhookservice.eventprocessing.PushEvent
import javax.inject.{Inject, Singleton}

import scala.language.higherKinds
import scala.util.Try
import scala.util.control.NonFatal

private class CommitEventsFinder[Interpretation[_]](
    commitInfoFinder: CommitInfoFinder[Interpretation]
)(implicit ME:        MonadError[Interpretation, Throwable]) {

  import commitInfoFinder._

  import Stream._

  def findCommitEvents(pushEvent: PushEvent): Interpretation[Stream[Interpretation[CommitEvent]]] =
    stream(List(pushEvent.after))(pushEvent)

  private def stream(
      commitIds:        List[CommitId]
  )(implicit pushEvent: PushEvent): Interpretation[Stream[Interpretation[CommitEvent]]] =
    commitIds match {
      case Nil =>
        ME.pure(Stream.empty[Interpretation[CommitEvent]])
      case commitId +: commitsToProcess => {
        for {
          commitEvent     <- findCommitEvent(commitId, pushEvent)
          nextCommitEvent <- stream(commitsToProcess ++ commitEvent.parents)
        } yield ME.pure(commitEvent) #:: nextCommitEvent
      } recoverWith { elementForFailure(commitsToProcess) }
    }

  private def elementForFailure(
      commitsToProcess: List[CommitId]
  )(implicit pushEvent: PushEvent): PartialFunction[Throwable, Interpretation[Stream[Interpretation[CommitEvent]]]] = {
    case NonFatal(exception) =>
      stream(commitsToProcess) map (ME.raiseError[CommitEvent](exception) #:: _)
  }

  private def findCommitEvent(commitId: CommitId, pushEvent: PushEvent): Interpretation[CommitEvent] =
    for {
      commitInfo  <- findCommitInfo(pushEvent.project.id, commitId)
      commitEvent <- merge(commitInfo, pushEvent)
    } yield commitEvent

  private def merge(commitInfo: CommitInfo, pushEvent: PushEvent): Interpretation[CommitEvent] = ME.fromTry {
    Try {
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
  }
}

@Singleton
private class IOCommitEventsFinder @Inject()(
    commitInfoFinder: IOCommitInfoFinder
) extends CommitEventsFinder[IO](commitInfoFinder)
