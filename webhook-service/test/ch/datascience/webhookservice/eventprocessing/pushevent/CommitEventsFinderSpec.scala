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

    "return single commit event if finding commit info returns no parents" in new TestCase {
      val pushEvent = pushEvents.generateOne

      val commitInfo = commitInfos(pushEvent.after, noParents).generateOne

      (commitInfoFinder
        .findCommitInfo(_: ProjectId, _: CommitId))
        .expects(pushEvent.project.id, pushEvent.after)
        .returning(context.pure(commitInfo))

      commitEventFinder.findCommitEvents(pushEvent).map(_.toList) shouldBe toSuccess(
        Seq(commitEventFrom(pushEvent, commitInfo))
      )
    }

    "return commit events starting from the push event's 'after' until commit info with no parents" in new TestCase {
      val pushEvent = pushEvents.generateOne

      val firstCommitInfo = commitInfos(pushEvent.after, parentsIdsLists(minNumber = 1)).generateOne

      val secondLevelCommitInfos = firstCommitInfo.parents map { parentId =>
        commitInfos(parentId, singleParent).generateOne
      }

      val thirdLevelCommitInfos = secondLevelCommitInfos.flatMap(_.parents) map { parentId =>
        commitInfos(parentId, noParents).generateOne
      }

      (Seq(firstCommitInfo) ++ secondLevelCommitInfos ++ thirdLevelCommitInfos) foreach { commitInfo =>
        (commitInfoFinder
          .findCommitInfo(_: ProjectId, _: CommitId))
          .expects(pushEvent.project.id, commitInfo.id)
          .returning(context.pure(commitInfo))
      }

      commitEventFinder.findCommitEvents(pushEvent).map(_.toList) shouldBe toSuccess(
        Seq(commitEventFrom(pushEvent, firstCommitInfo)),
        secondLevelCommitInfos.map(commitEventFrom(pushEvent, _)),
        secondLevelCommitInfos.flatMap(_.parents).map(commitEventFrom(pushEvent, thirdLevelCommitInfos))
      )
    }

    "fail if finding the first commit info fails" in new TestCase {
      val pushEvent = pushEvents.generateOne

      val exception = exceptions.generateOne
      (commitInfoFinder
        .findCommitInfo(_: ProjectId, _: CommitId))
        .expects(pushEvent.project.id, pushEvent.after)
        .returning(context.raiseError(exception))

      commitEventFinder.findCommitEvents(pushEvent).map(_.toList) shouldBe Success(
        Seq(
          Failure(exception)
        )
      )
    }

    "fail if finding one of the commit info fails" in new TestCase {
      val pushEvent = pushEvents.generateOne

      val firstCommitInfo = commitInfos(pushEvent.after, parentsIdsLists(minNumber = 2, maxNumber = 2)).generateOne

      val secondLevelCommitInfo1 +: secondLevelCommitInfo2 +: Nil = firstCommitInfo.parents map { parentId =>
        commitInfos(parentId, noParents).generateOne
      }

      Seq(firstCommitInfo, secondLevelCommitInfo2) foreach { commitInfo =>
        (commitInfoFinder
          .findCommitInfo(_: ProjectId, _: CommitId))
          .expects(pushEvent.project.id, commitInfo.id)
          .returning(context.pure(commitInfo))
      }

      val exception = exceptions.generateOne
      (commitInfoFinder
        .findCommitInfo(_: ProjectId, _: CommitId))
        .expects(pushEvent.project.id, secondLevelCommitInfo1.id)
        .returning(context.raiseError(exception))

      commitEventFinder.findCommitEvents(pushEvent).map(_.toList) shouldBe Success(
        Seq(
          Success(commitEventFrom(pushEvent, firstCommitInfo)),
          Failure(exception),
          Success(commitEventFrom(pushEvent, secondLevelCommitInfo2))
        )
      )
    }
  }

  private trait TestCase {
    val context: MonadError[Try, Throwable] = MonadError[Try, Throwable]

    val commitInfoFinder  = mock[CommitInfoFinder[Try]]
    val commitEventFinder = new CommitEventsFinder[Try](commitInfoFinder)
  }

  private def commitEventFrom(pushEvent: PushEvent, commitInfo: CommitInfo) = CommitEvent(
    id            = commitInfo.id,
    message       = commitInfo.message,
    committedDate = commitInfo.committedDate,
    pushUser      = pushEvent.pushUser,
    author        = commitInfo.author,
    committer     = commitInfo.committer,
    parents       = commitInfo.parents,
    project       = pushEvent.project
  )

  private def commitEventFrom(
      pushEvent:         PushEvent,
      parentCommitInfos: Seq[CommitInfo]
  )(parentId:            CommitId): CommitEvent =
    commitEventFrom(
      pushEvent = pushEvent,
      commitInfo = parentCommitInfos
        .find(_.id == parentId)
        .getOrElse(throw new Exception(s"No commitInfo for $parentId"))
    )

  private def toSuccess(commitEvents: Seq[CommitEvent]*) = Success(
    commitEvents.flatten map Success.apply
  )

  private def commitInfos(commitId: CommitId, parentsGenerator: Gen[List[CommitId]]): Gen[CommitInfo] =
    for {
      message       <- commitMessages
      committedDate <- committedDates
      author        <- users
      committer     <- users
      parentsIds    <- parentsGenerator
    } yield
      CommitInfo(
        id            = commitId,
        message       = message,
        committedDate = committedDate,
        author        = author,
        committer     = committer,
        parents       = parentsIds
      )

  private val noParents: Gen[List[CommitId]] = Gen.const(List.empty)

  private val singleParent: Gen[List[CommitId]] = parentsIdsLists(minNumber = 1, maxNumber = 1)
}
