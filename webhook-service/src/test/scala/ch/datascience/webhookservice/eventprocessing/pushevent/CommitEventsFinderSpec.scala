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
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events._
import ch.datascience.http.client.AccessToken
import ch.datascience.webhookservice.eventprocessing.PushEvent
import ch.datascience.webhookservice.generators.WebhookServiceGenerators.pushEvents
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Failure, Success, Try}

class CommitEventsFinderSpec extends WordSpec with MockFactory {

  "findCommitEvents" should {

    "return single commit event " +
      "if found commit info has no parents " +
      "and there are no events in the Event Log" in new TestCase {

      givenLatestEventInTheLog(None)

      val commitInfo = commitInfos(pushEvent.commitTo, noParents).generateOne
      givenFindingCommitInfoReturns(commitInfo)

      commitEventFinder.findCommitEvents(pushEvent, maybeAccessToken).map(_.toList) shouldBe toSuccess(
        List(commitEventFrom(pushEvent, commitInfo))
      )
    }

    "return no commit events " +
      "if Push Event's 'commitTo' matches the youngest event from the Event Log" in new TestCase {

      givenLatestEventInTheLog(Some(pushEvent.commitTo))

      commitEventFinder.findCommitEvents(pushEvent, maybeAccessToken).map(_.toList) shouldBe toSuccess(List.empty)
    }

    "return commit events starting from the 'commitTo' to the youngest event in the Event Log" in new TestCase {

      val level1Info = commitInfos(pushEvent.commitTo, singleParent).generateOne

      val level2Infos = level1Info.parents map { parentId =>
        commitInfos(parentId, singleParent).generateOne
      }

      val level3Infos = level2Infos.flatMap(_.parents) map { parentId =>
        commitInfos(parentId, noParents).generateOne
      }

      givenLatestEventInTheLog(level3Infos.headOption.map(_.id))

      givenFindingCommitInfoReturns(level1Info, level2Infos)

      commitEventFinder.findCommitEvents(pushEvent, maybeAccessToken).map(_.toList) shouldBe toSuccess(
        List(commitEventFrom(pushEvent, level1Info)),
        level2Infos map (commitEventFrom(pushEvent, _))
      )
    }

    "return commit events starting from the 'commitTo' to the oldest ancestor " +
      "if there are no events in the Event Log" in new TestCase {

      givenLatestEventInTheLog(None)

      val level1Info = commitInfos(pushEvent.commitTo, singleParent).generateOne

      val level2Infos = level1Info.parents map { parentId =>
        commitInfos(parentId, singleParent).generateOne
      }

      val level3Infos = level2Infos.flatMap(_.parents) map { parentId =>
        commitInfos(parentId, noParents).generateOne
      }

      givenFindingCommitInfoReturns(level1Info, level2Infos, level3Infos)

      commitEventFinder.findCommitEvents(pushEvent, maybeAccessToken).map(_.toList) shouldBe toSuccess(
        List(commitEventFrom(pushEvent, level1Info)),
        level2Infos map (commitEventFrom(pushEvent, _)),
        level2Infos.flatMap(_.parents) map commitEventFrom(pushEvent, level3Infos)
      )
    }

    "return commit events starting from the 'commitTo' to the oldest ancestor and multiple parents, " +
      "skipping ids already in the Event Log" in new TestCase {

      val level1Parent1 = commitIds.generateOne
      val level1Parent2 = commitIds.generateOne
      val level1Info    = commitInfos(pushEvent.commitTo, level1Parent1, level1Parent2).generateOne

      val level2Commit2Parent = commitIds.generateOne
      val level2Infos @ _ +: level2Info2 +: Nil = List(
        commitInfos(level1Parent1, parents = commitIds.generateOne).generateOne,
        commitInfos(level1Parent2, parents = level2Commit2Parent).generateOne
      )

      givenLatestEventInTheLog(Some(level2Commit2Parent))

      givenFindingCommitInfoReturns(level1Info, level2Info2)

      (eventLogVerifyExistence
        .filterNotExistingInLog(_: List[CommitId], _: ProjectId))
        .expects(List(level1Parent1, level1Parent2), pushEvent.project.id)
        .returning(context.pure(List(level1Parent2)))

      commitEventFinder.findCommitEvents(pushEvent, maybeAccessToken).map(_.toList) shouldBe toSuccess(
        List(
          commitEventFrom(pushEvent, level1Info),
          commitEventFrom(pushEvent, level2Info2)
        )
      )
    }

    "return commit events starting from the 'commitTo' to the oldest ancestor " +
      "skipping commits with the 'don't care' 0000000000000000000000000000000000000000 ref" in new TestCase {

      givenLatestEventInTheLog(None)

      val level1Parent1 = CommitId("0000000000000000000000000000000000000000")
      val level1Parent2 = commitIds.generateOne
      val level1Info    = commitInfos(pushEvent.commitTo, level1Parent1, level1Parent2).generateOne
      val level2Info    = commitInfos(level1Parent2, noParents).generateOne

      givenFindingCommitInfoReturns(level1Info, level2Info)

      commitEventFinder.findCommitEvents(pushEvent, maybeAccessToken).map(_.toList) shouldBe toSuccess(
        List(
          commitEventFrom(pushEvent, level1Info.copy(parents = List(level1Parent2))),
          commitEventFrom(pushEvent, level2Info),
        )
      )
    }

    "fail if verifying existence of parent ids in the Event Log fails" in new TestCase {

      givenLatestEventInTheLog(None)

      val parents    = parentsIdsLists(minNumber = 2).generateOne
      val commitInfo = commitInfos(pushEvent.commitTo, parents: _*).generateOne

      givenFindingCommitInfoReturns(commitInfo)

      val exception = exceptions.generateOne
      (eventLogVerifyExistence
        .filterNotExistingInLog(_: List[CommitId], _: ProjectId))
        .expects(parents, pushEvent.project.id)
        .returning(context.raiseError(exception))

      commitEventFinder.findCommitEvents(pushEvent, maybeAccessToken) shouldBe context.raiseError(exception)
    }

    "fail if finding the latest event in Event Log fails" in new TestCase {

      val exception = exceptions.generateOne
      (eventLogLatestEvent
        .findYoungestEventInLog(_: ProjectId))
        .expects(pushEvent.project.id)
        .returning(context.raiseError(exception))

      commitEventFinder.findCommitEvents(pushEvent, maybeAccessToken) shouldBe context.raiseError(exception)
    }

    "return a stream with a single failure item if finding the first commit info fails" in new TestCase {

      givenLatestEventInTheLog(None)

      val exception = exceptions.generateOne
      (commitInfoFinder
        .findCommitInfo(_: ProjectId, _: CommitId, _: Option[AccessToken]))
        .expects(pushEvent.project.id, pushEvent.commitTo, maybeAccessToken)
        .returning(context.raiseError(exception))

      commitEventFinder.findCommitEvents(pushEvent, maybeAccessToken).map(_.toList) shouldBe Success(
        Seq(
          Failure(exception)
        )
      )
    }

    "return commit events and a failure item for commit for which finding the commit info failed" in new TestCase {

      givenLatestEventInTheLog(None)

      val level1Info = commitInfos(pushEvent.commitTo, parentsIdsLists(minNumber = 2, maxNumber = 2)).generateOne

      (eventLogVerifyExistence
        .filterNotExistingInLog(_: List[CommitId], _: ProjectId))
        .expects(level1Info.parents, pushEvent.project.id)
        .returning(context.pure(level1Info.parents))

      val level2Info1 +: level2Info2 +: Nil = level1Info.parents map { parentId =>
        commitInfos(parentId, noParents).generateOne
      }

      givenFindingCommitInfoReturns(level1Info, level2Info2)

      val exception = exceptions.generateOne
      (commitInfoFinder
        .findCommitInfo(_: ProjectId, _: CommitId, _: Option[AccessToken]))
        .expects(pushEvent.project.id, level2Info1.id, maybeAccessToken)
        .returning(context.raiseError(exception))

      commitEventFinder.findCommitEvents(pushEvent, maybeAccessToken).map(_.toList) shouldBe Success(
        Seq(
          Success(commitEventFrom(pushEvent, level1Info)),
          Failure(exception),
          Success(commitEventFrom(pushEvent, level2Info2))
        )
      )
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val pushEvent        = pushEvents.generateOne
    val maybeAccessToken = Gen.option(accessTokens).generateOne

    val commitInfoFinder        = mock[CommitInfoFinder[Try]]
    val eventLogLatestEvent     = mock[TryEventLogLatestEvent]
    val eventLogVerifyExistence = mock[TryEventLogVerifyExistence]
    val commitEventFinder       = new CommitEventsFinder[Try](commitInfoFinder, eventLogLatestEvent, eventLogVerifyExistence)

    def givenLatestEventInTheLog(maybeEventId: Option[CommitId]): Unit =
      (eventLogLatestEvent
        .findYoungestEventInLog(_: ProjectId))
        .expects(pushEvent.project.id)
        .returning(context.pure(maybeEventId))

    def givenFindingCommitInfoReturns(commitInfo: CommitInfo, otherInfos: Seq[CommitInfo]*): Unit =
      givenFindingCommitInfoReturns(commitInfo +: otherInfos.flatten: _*)

    def givenFindingCommitInfoReturns(commitInfos: CommitInfo*): Unit =
      commitInfos foreach { commitInfo =>
        (commitInfoFinder
          .findCommitInfo(_: ProjectId, _: CommitId, _: Option[AccessToken]))
          .expects(pushEvent.project.id, commitInfo.id, maybeAccessToken)
          .returning(context.pure(commitInfo))

      }
  }

  private def commitEventFrom(pushEvent: PushEvent, commitInfo: CommitInfo) =
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

  private def toSuccess(commitEvents: List[CommitEvent]*) = Success(
    commitEvents.flatten map Success.apply
  )

  private def commitInfos(commitId: CommitId, parents: CommitId*): Gen[CommitInfo] =
    commitInfos(commitId, Gen.const(parents.toList))

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
