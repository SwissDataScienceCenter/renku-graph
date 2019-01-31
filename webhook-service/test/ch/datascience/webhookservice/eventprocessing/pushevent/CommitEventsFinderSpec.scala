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
import ch.datascience.graph.events.GraphCommonsGenerators._
import ch.datascience.graph.events._
import ch.datascience.webhookservice.eventprocessing.CommitEventsOrigin
import ch.datascience.webhookservice.generators.ServiceTypesGenerators.commitEventsOrigins
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Failure, Success, Try}

class CommitEventsFinderSpec extends WordSpec with MockFactory {

  "findCommitEvents" should {

    "return single commit event if finding commit info returns no parents" in new TestCase {
      val commitEventsOrigin = commitEventsOrigins.generateOne
      val commitInfo         = commitInfos(commitEventsOrigin.commitTo, noParents).generateOne

      (commitInfoFinder
        .findCommitInfo(_: ProjectId, _: CommitId))
        .expects(commitEventsOrigin.project.id, commitEventsOrigin.commitTo)
        .returning(context.pure(commitInfo))

      commitEventFinder.findCommitEvents(commitEventsOrigin).map(_.toList) shouldBe toSuccess(
        Seq(commitEventFrom(commitEventsOrigin, commitInfo))
      )
    }

    "return commit events starting from the 'commitTo' until commit info with no parents" in new TestCase {
      val commitEventsOrigin = commitEventsOrigins.generateOne.copy(maybeCommitFrom = None)

      val firstCommitInfo = commitInfos(commitEventsOrigin.commitTo, parentsIdsLists(minNumber = 1)).generateOne

      val secondLevelCommitInfos = firstCommitInfo.parents map { parentId =>
        commitInfos(parentId, singleParent).generateOne
      }

      val thirdLevelCommitInfos = secondLevelCommitInfos.flatMap(_.parents) map { parentId =>
        commitInfos(parentId, noParents).generateOne
      }

      (Seq(firstCommitInfo) ++ secondLevelCommitInfos ++ thirdLevelCommitInfos) foreach { commitInfo =>
        (commitInfoFinder
          .findCommitInfo(_: ProjectId, _: CommitId))
          .expects(commitEventsOrigin.project.id, commitInfo.id)
          .returning(context.pure(commitInfo))
      }

      commitEventFinder.findCommitEvents(commitEventsOrigin).map(_.toList) shouldBe toSuccess(
        Seq(commitEventFrom(commitEventsOrigin, firstCommitInfo)),
        secondLevelCommitInfos map (commitEventFrom(commitEventsOrigin, _)),
        secondLevelCommitInfos.flatMap(_.parents) map commitEventFrom(commitEventsOrigin, thirdLevelCommitInfos)
      )
    }

    "return commit events starting from the 'commitTo' until found commit id matches the `commitTo`" in new TestCase {
      val commitTo = commitIds.generateOne

      val firstCommitInfo = commitInfos(commitTo, parentsIdsLists(minNumber = 3)).generateOne

      val secondLevelCommitInfos = firstCommitInfo.parents map { parentId =>
        commitInfos(parentId, noParents).generateOne
      }

      val commitFrom = firstCommitInfo.parents(firstCommitInfo.parents.size - 2)
      val commitEventsOrigin = commitEventsOrigins.generateOne.copy(
        maybeCommitFrom = Some(commitFrom),
        commitTo        = commitTo
      )

      val commitInfosUpToCommitFrom = secondLevelCommitInfos.takeWhile(_.id != commitFrom)
      (Seq(firstCommitInfo) ++ commitInfosUpToCommitFrom) foreach { commitInfo =>
        (commitInfoFinder
          .findCommitInfo(_: ProjectId, _: CommitId))
          .expects(commitEventsOrigin.project.id, commitInfo.id)
          .returning(context.pure(commitInfo))
      }

      commitEventFinder.findCommitEvents(commitEventsOrigin).map(_.toList) shouldBe toSuccess(
        Seq(commitEventFrom(commitEventsOrigin, firstCommitInfo)),
        commitInfosUpToCommitFrom map (commitEventFrom(commitEventsOrigin, _))
      )
    }

    "fail if finding the first commit info fails" in new TestCase {
      val commitEventsOrigin = commitEventsOrigins.generateOne

      val exception = exceptions.generateOne
      (commitInfoFinder
        .findCommitInfo(_: ProjectId, _: CommitId))
        .expects(commitEventsOrigin.project.id, commitEventsOrigin.commitTo)
        .returning(context.raiseError(exception))

      commitEventFinder.findCommitEvents(commitEventsOrigin).map(_.toList) shouldBe Success(
        Seq(
          Failure(exception)
        )
      )
    }

    "fail if finding one of the commit info fails" in new TestCase {
      val commitEventsOrigin = commitEventsOrigins.generateOne

      val firstCommitInfo =
        commitInfos(commitEventsOrigin.commitTo, parentsIdsLists(minNumber = 2, maxNumber = 2)).generateOne

      val secondLevelCommitInfo1 +: secondLevelCommitInfo2 +: Nil = firstCommitInfo.parents map { parentId =>
        commitInfos(parentId, noParents).generateOne
      }

      Seq(firstCommitInfo, secondLevelCommitInfo2) foreach { commitInfo =>
        (commitInfoFinder
          .findCommitInfo(_: ProjectId, _: CommitId))
          .expects(commitEventsOrigin.project.id, commitInfo.id)
          .returning(context.pure(commitInfo))
      }

      val exception = exceptions.generateOne
      (commitInfoFinder
        .findCommitInfo(_: ProjectId, _: CommitId))
        .expects(commitEventsOrigin.project.id, secondLevelCommitInfo1.id)
        .returning(context.raiseError(exception))

      commitEventFinder.findCommitEvents(commitEventsOrigin).map(_.toList) shouldBe Success(
        Seq(
          Success(commitEventFrom(commitEventsOrigin, firstCommitInfo)),
          Failure(exception),
          Success(commitEventFrom(commitEventsOrigin, secondLevelCommitInfo2))
        )
      )
    }
  }

  private trait TestCase {
    val context: MonadError[Try, Throwable] = MonadError[Try, Throwable]

    val hookAccessToken = hookAccessTokens.generateOne

    val commitInfoFinder  = mock[CommitInfoFinder[Try]]
    val commitEventFinder = new CommitEventsFinder[Try](commitInfoFinder)
  }

  private def commitEventFrom(commitEventsOrigin: CommitEventsOrigin, commitInfo: CommitInfo) =
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

  private def commitEventFrom(
      commitEventsOrigin: CommitEventsOrigin,
      parentCommitInfos:  Seq[CommitInfo]
  )(parentId:             CommitId): CommitEvent =
    commitEventFrom(
      commitEventsOrigin = commitEventsOrigin,
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
