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

package ch.datascience.webhookservice.eventprocessing.startcommit

import cats.MonadError
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events._
import ch.datascience.http.client.AccessToken
import ch.datascience.webhookservice.commits.{CommitInfo, CommitInfoFinder}
import ch.datascience.webhookservice.generators.WebhookServiceGenerators
import ch.datascience.webhookservice.generators.WebhookServiceGenerators.startCommits
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Success, Try}

class CommitEventsSourceBuilderSpec extends WordSpec with MockFactory {

  "newCommitEventSource.transformEventsWith" should {

    "map a single commit event with the transform function " +
      "if found commit info has no parents " +
      "and there are no events in the Event Log" in new TestCase {

      givenNonExistingInLog(in = startCommit.id, out = startCommit.id)

      val commitInfo = commitInfos(startCommit.id, noParents).generateOne
      givenFindingCommitInfoReturns(commitInfo)

      source.transformEventsWith(send) shouldBe context.pure(List(commitInfo.id))
    }

    "do not map any commit events with the transform function " +
      "if Start Commit's 'commitTo' matches the latest event from the Event Log" in new TestCase {

      givenNonExistingInLog(in = List(startCommit.id), out = Nil)

      source.transformEventsWith(send) shouldBe context.pure(Nil)
    }

    "map commit events with the transform function " +
      "starting from the 'commitTo' to the ancestor matching the latest event in the Event Log" in new TestCase {

      val level1Info = commitInfos(startCommit.id, singleParent).generateOne

      val level2Infos = level1Info.parents map { parentId =>
        commitInfos(parentId, singleParent).generateOne
      }

      val level3Infos = level2Infos.flatMap(_.parents) map { parentId =>
        commitInfos(parentId, noParents).generateOne
      }

      givenFindingCommitInfoReturns(level1Info, level2Infos)

      givenNonExistingInLog(in = level1Info.id, out          = level1Info.id)
      givenNonExistingInLog(in = level2Infos map (_.id), out = level2Infos map (_.id))
      givenNonExistingInLog(in = level3Infos map (_.id), out = Nil)

      source.transformEventsWith(send) shouldBe context.pure(level1Info +: level2Infos map (_.id))
    }

    "map commit events with the transform function " +
      "starting from the 'commitTo' to the oldest ancestor " +
      "if there are no events in the Event Log" in new TestCase {

      val level1Info = commitInfos(startCommit.id, singleParent).generateOne

      val level2Infos = level1Info.parents map { parentId =>
        commitInfos(parentId, singleParent).generateOne
      }

      val level3Infos = level2Infos.flatMap(_.parents) map { parentId =>
        commitInfos(parentId, noParents).generateOne
      }

      givenFindingCommitInfoReturns(level1Info, level2Infos, level3Infos)

      givenNonExistingInLog(in = level1Info.id, out          = level1Info.id)
      givenNonExistingInLog(in = level2Infos map (_.id), out = level2Infos map (_.id))
      givenNonExistingInLog(in = level3Infos map (_.id), out = level3Infos map (_.id))

      source.transformEventsWith(send) shouldBe context.pure(level1Info +: level2Infos ++: level3Infos map (_.id))
    }

    "map commit events with the transform function " +
      "starting from the 'commitTo' to the oldest ancestor and multiple parents, " +
      "skipping ids already in the Event Log" in new TestCase {

      val level1Parent1 = commitIds.generateOne
      val level1Parent2 = commitIds.generateOne
      val level1Info    = commitInfos(startCommit.id, level1Parent1, level1Parent2).generateOne

      val level2Commit2Parent = commitIds.generateOne
      val level2Infos @ _ +: level2Info2 +: Nil = List(
        commitInfos(level1Parent1, parents = commitIds.generateOne).generateOne,
        commitInfos(level1Parent2, parents = level2Commit2Parent).generateOne
      )

      givenFindingCommitInfoReturns(level1Info, level2Info2)

      givenNonExistingInLog(in = level1Info.id, out             = level1Info.id)
      givenNonExistingInLog(in = level2Infos map (_.id), out    = List(level1Parent2))
      givenNonExistingInLog(in = List(level2Commit2Parent), out = Nil)

      source.transformEventsWith(send) shouldBe context.pure(level1Info +: level2Info2 +: Nil map (_.id))
    }

    "map commit events with the transform function " +
      "starting from the 'commitTo' to the oldest ancestor " +
      "skipping commits with the 'don't care' 0000000000000000000000000000000000000000 ref" in new TestCase {

      val level1Parent1 = CommitId("0000000000000000000000000000000000000000")
      val level1Parent2 = commitIds.generateOne
      val level1Info    = commitInfos(startCommit.id, level1Parent1, level1Parent2).generateOne
      val level2Info    = commitInfos(level1Parent2, noParents).generateOne

      givenFindingCommitInfoReturns(level1Info, level2Info)

      givenNonExistingInLog(in = level1Info.id, out = level1Info.id)
      givenNonExistingInLog(in = level2Info.id, out = level2Info.id)

      source.transformEventsWith(send) shouldBe context.pure(level1Info +: level2Info +: Nil map (_.id))
    }

    "fail mapping the Commit Events " +
      "if verifying existence of parent ids in the Event Log fails" in new TestCase {

      val commitInfo = commitInfos(startCommit.id, parentsIdsLists(minNumber = 2)).generateOne

      val exception = exceptions.generateOne
      (eventLogVerifyExistence
        .filterNotExistingInLog(_: List[CommitId], _: ProjectId))
        .expects(List(commitInfo.id), startCommit.project.id)
        .returning(context.raiseError(exception))

      source.transformEventsWith(send) shouldBe context.raiseError(exception)
    }

    "fail mapping the Commit Events " +
      "if finding commit info fails" in new TestCase {

      val level1Info = commitInfos(startCommit.id, parentsIdsLists(minNumber = 2, maxNumber = 2)).generateOne
      val level2Infos @ level2Info1 +: level2Info2 +: Nil = level1Info.parents map { parentId =>
        commitInfos(parentId, noParents).generateOne
      }

      givenFindingCommitInfoReturns(level1Info, level2Info2)

      val exception = exceptions.generateOne
      (commitInfoFinder
        .findCommitInfo(_: ProjectId, _: CommitId, _: Option[AccessToken]))
        .expects(startCommit.project.id, level2Info1.id, maybeAccessToken)
        .returning(context.raiseError(exception))

      givenNonExistingInLog(in = level1Info.id, out          = level1Info.id)
      givenNonExistingInLog(in = level2Infos map (_.id), out = level2Infos map (_.id))

      source.transformEventsWith(send) shouldBe context.raiseError(exception)
    }

    "fail mapping the Commit Events " +
      "if the transform function fails during transformation one of the events" in new TestCase {

      val level1Parent1 = commitIds.generateOne
      val level1Parent2 = commitIds.generateOne
      val level1Info    = commitInfos(startCommit.id, level1Parent1, level1Parent2).generateOne

      val level2Infos @ level2Info1 +: _ = List(
        commitInfos(level1Parent1, noParents).generateOne,
        commitInfos(level1Parent2, noParents).generateOne
      )
      givenFindingCommitInfoReturns(level1Info, level2Infos)

      givenNonExistingInLog(in = level1Info.id, out          = level1Info.id)
      givenNonExistingInLog(in = level2Infos map (_.id), out = level2Infos map (_.id))

      val exception = exceptions.generateOne
      val failingSend: CommitEvent => Try[CommitId] = event =>
        if (event.id == level2Info1.id) context.raiseError(exception)
        else context.pure(event.id)
      source.transformEventsWith(failingSend) shouldBe context.raiseError(exception)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val startCommit      = startCommits.generateOne
    val maybeAccessToken = Gen.option(accessTokens).generateOne

    val send: CommitEvent => Try[CommitId] = event => Try(event.id)
    val commitInfoFinder        = mock[CommitInfoFinder[Try]]
    val eventLogVerifyExistence = mock[TryEventLogVerifyExistence]
    private val sourceBuilder   = new CommitEventsSourceBuilder[Try](commitInfoFinder, eventLogVerifyExistence)
    val Success(source)         = sourceBuilder.buildEventsSource(startCommit, maybeAccessToken)

    def givenFindingCommitInfoReturns(commitInfo: CommitInfo, otherInfos: Seq[CommitInfo]*): Unit =
      givenFindingCommitInfoReturns(commitInfo +: otherInfos.flatten: _*)

    def givenFindingCommitInfoReturns(commitInfos: CommitInfo*): Unit =
      commitInfos foreach { commitInfo =>
        (commitInfoFinder
          .findCommitInfo(_: ProjectId, _: CommitId, _: Option[AccessToken]))
          .expects(startCommit.project.id, commitInfo.id, maybeAccessToken)
          .returning(context pure commitInfo)
      }

    def givenNonExistingInLog(in: CommitId, out: CommitId): Unit =
      givenNonExistingInLog(List(in), List(out))

    def givenNonExistingInLog(in: List[CommitId], out: List[CommitId]): Unit =
      (eventLogVerifyExistence
        .filterNotExistingInLog(_: List[CommitId], _: ProjectId))
        .expects(in, startCommit.project.id)
        .returning(context pure out)
  }

  private def commitInfos(commitId: CommitId, parents: CommitId*): Gen[CommitInfo] =
    commitInfos(commitId, Gen.const(parents.toList))

  private def commitInfos(commitId: CommitId, parentsGenerator: Gen[List[CommitId]]): Gen[CommitInfo] =
    for {
      info       <- WebhookServiceGenerators.commitInfos
      parentsIds <- parentsGenerator
    } yield info.copy(id = commitId, parents = parentsIds)

  private val noParents: Gen[List[CommitId]] = Gen.const(List.empty)

  private val singleParent: Gen[List[CommitId]] = parentsIdsLists(minNumber = 1, maxNumber = 1)
}
