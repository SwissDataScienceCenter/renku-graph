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
import ch.datascience.graph.events.CommitEvent
import ch.datascience.webhookservice.eventprocessing.PushEvent
import javax.inject.{Inject, Singleton}

import scala.language.higherKinds
import scala.util.Try

private class CommitEventsFinder[Interpretation[_]](
    commitInfoFinder: CommitInfoFinder[Interpretation]
)(implicit ME:        MonadError[Interpretation, Throwable]) {

  import commitInfoFinder._

  def findCommitEvents(pushEvent: PushEvent): Interpretation[CommitEvent] =
    for {
      commitInfo  <- findCommitInfo(pushEvent.project.id, pushEvent.after)
      commitEvent <- merge(pushEvent, commitInfo)
    } yield commitEvent

  private def merge(pushEvent: PushEvent, commitInfo: CommitInfo): Interpretation[CommitEvent] = ME.fromTry {
    Try {
      CommitEvent(
        id            = pushEvent.after,
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
