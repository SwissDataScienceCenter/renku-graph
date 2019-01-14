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
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events._
import ch.datascience.webhookservice.eventprocessing.PushEvent
import ch.datascience.webhookservice.generators.ServiceTypesGenerators.pushEvents
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Failure, Success, Try}

class CommitEventsFinderSpec extends WordSpec with MockFactory {

  "findCommitEvents" should {

    "find commit info and combine that with the given push event to return commit event" in new TestCase {
      val pushEvent = pushEvents.generateOne

      val commitInfo = commitInfos(pushEvent).generateOne
      (commitInfoFinder
        .findCommitInfo(_: ProjectId, _: CommitId))
        .expects(pushEvent.project.id, pushEvent.after)
        .returning(context.pure(commitInfo))

      commitEventFinder.findCommitEvents(pushEvent) shouldBe Success(commitEventFrom(pushEvent, commitInfo))
    }

    "fail if finding commit info fails" in new TestCase {
      val pushEvent = pushEvents.generateOne

      val exception = exceptions.generateOne
      (commitInfoFinder
        .findCommitInfo(_: ProjectId, _: CommitId))
        .expects(pushEvent.project.id, pushEvent.after)
        .returning(context.raiseError(exception))

      commitEventFinder.findCommitEvents(pushEvent) shouldBe Failure(exception)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val commitInfoFinder  = mock[CommitInfoFinder[Try]]
    val commitEventFinder = new CommitEventsFinder[Try](commitInfoFinder)
  }

  private def commitEventFrom(pushEvent: PushEvent, commitInfo: CommitInfo) = CommitEvent(
    id            = pushEvent.after,
    message       = commitInfo.message,
    committedDate = commitInfo.committedDate,
    pushUser      = pushEvent.pushUser,
    author        = commitInfo.author,
    committer     = commitInfo.committer,
    parents       = commitInfo.parents,
    project       = pushEvent.project
  )

  private def commitInfos(pushEvent: PushEvent): Gen[CommitInfo] =
    for {
      message       <- commitMessages
      committedDate <- committedDates
      author        <- users
      committer     <- users
      parentsIds    <- parentsIdsLists
    } yield
      CommitInfo(
        id            = pushEvent.after,
        message       = message,
        committedDate = committedDate,
        author        = author,
        committer     = committer,
        parents       = parentsIds
      )

}
