/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import java.time.{Clock, Instant, ZoneId}

import cats.MonadError
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects.Id
import ch.datascience.http.client.AccessToken
import ch.datascience.webhookservice.commits.{CommitInfo, CommitInfoFinder}
import ch.datascience.webhookservice.eventprocessing.CommitEvent
import ch.datascience.webhookservice.eventprocessing.startcommit.SendingResult._
import ch.datascience.webhookservice.generators.WebhookServiceGenerators
import ch.datascience.webhookservice.generators.WebhookServiceGenerators._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class CommitEventsSourceBuilderSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "newCommitEventSource.transformEventsWith" should {

    "map a single commit event with the transform function " +
      "if found commit info has no parents " +
      "and the event get created in the Event Log" in new TestCase {

      val commitInfo = commitInfos(startCommit.id, noParents).generateOne
      givenFindingCommitInfoReturns(commitInfo)

      source.transformEventsWith(send(returning = startCommit.id -> Created)) shouldBe List(Created).pure[Try]
    }

    "map a single commit event with the transform function " +
      "if found commit info has no parents " +
      "and the event already exists in the Event Log" in new TestCase {

      val commitInfo = commitInfos(startCommit.id, noParents).generateOne
      givenFindingCommitInfoReturns(commitInfo)

      source.transformEventsWith(send(returning = startCommit.id -> Existed)) shouldBe List(Existed).pure[Try]
    }

    "map a single commit event with the transform function " +
      "if found commit info has some parents " +
      "but the event for the start event already exists in the Event Log" in new TestCase {

      val commitInfo = commitInfos(startCommit.id, someParents).generateOne
      givenFindingCommitInfoReturns(commitInfo)

      source.transformEventsWith(send(returning = startCommit.id -> Existed)) shouldBe List(Existed).pure[Try]
    }

    "map commit events with the transform function " +
      "starting from the given start commit to the ancestor matching the latest event in the Event Log" in new TestCase {

      val level1Info = commitInfos(startCommit.id, singleParent).generateOne

      val level2Infos = level1Info.parents map { parentId =>
        commitInfos(parentId, singleParent).generateOne
      }

      val level3Infos = level2Infos.flatMap(_.parents) map { parentId =>
        commitInfos(parentId, noParents).generateOne
      }

      givenFindingCommitInfoReturns(level1Info, level2Infos)

      source.transformEventsWith(
        send(returning = startCommit.id -> Created, level2Infos.head.id -> Existed)
      ) shouldBe List(Created, Existed).pure[Try]
    }

    "map commit events with the transform function " +
      "starting from the given start commit to the oldest ancestor " +
      "if there are no events in the Event Log" in new TestCase {

      val level1Parent = commitIds.generateOne
      val level1Info   = commitInfos(startCommit.id, level1Parent).generateOne

      val level2Parent = commitIds.generateOne
      val level2Infos = List(
        commitInfos(level1Parent, level2Parent).generateOne
      )

      val level3Infos = List(
        commitInfos(level2Parent, noParents).generateOne
      )

      givenFindingCommitInfoReturns(level1Info, level2Infos, level3Infos)

      source.transformEventsWith(
        send(returning = startCommit.id -> Created, level1Parent -> Created, level2Parent -> Created)
      ) shouldBe List(Created, Created, Created).pure[Try]
    }

    "map commit events with the transform function " +
      "starting from the given start commit to the oldest ancestor and multiple parents, " +
      "skipping traversing history of commits already in the Event Log" in new TestCase {

      val level1Parent1 = commitIds.generateOne
      val level1Parent2 = commitIds.generateOne
      val level1Info    = commitInfos(startCommit.id, level1Parent1, level1Parent2).generateOne

      val level2Parent = commitIds.generateOne
      val level2Infos = List(
        commitInfos(level1Parent1, someParents).generateOne,
        commitInfos(level1Parent2, level2Parent).generateOne
      )
      val level3Info = commitInfos(level2Parent, noParents).generateOne

      givenFindingCommitInfoReturns(level1Info, level2Infos, List(level3Info))

      source.transformEventsWith(
        send(
          startCommit.id -> Created,
          level1Parent1  -> Existed,
          level1Parent2  -> Created,
          level2Parent   -> Created
        )
      ) shouldBe List(Created, Existed, Created, Created).pure[Try]
    }

    "map commit events with the transform function " +
      "starting from given start commit to the oldest ancestor " +
      "skipping commits with the 'don't care' 0000000000000000000000000000000000000000 ref" in new TestCase {

      val level1Parent1 = CommitId("0000000000000000000000000000000000000000")
      val level1Parent2 = commitIds.generateOne
      val level1Info    = commitInfos(startCommit.id, level1Parent1, level1Parent2).generateOne
      val level2Info    = commitInfos(level1Parent2, noParents).generateOne

      givenFindingCommitInfoReturns(level1Info, level2Info)

      source.transformEventsWith(
        send(
          startCommit.id -> Created,
          level1Parent2  -> Created
        )
      ) shouldBe List(Created, Created).pure[Try]
    }

    "fail mapping the events " +
      "if finding commit info fails" in new TestCase {

      val level1Info = commitInfos(startCommit.id, parentsIdsLists(minNumber = 2, maxNumber = 2)).generateOne
      val level2Infos @ level2Info1 +: level2Info2 +: Nil = level1Info.parents map { parentId =>
        commitInfos(parentId, noParents).generateOne
      }

      givenFindingCommitInfoReturns(level1Info, level2Info2)

      val exception = exceptions.generateOne
      (commitInfoFinder
        .findCommitInfo(_: Id, _: CommitId, _: Option[AccessToken]))
        .expects(startCommit.project.id, level2Info1.id, maybeAccessToken)
        .returning(context.raiseError(exception))

      source.transformEventsWith(
        send(
          startCommit.id -> Created,
          level2Info2.id -> Created
        )
      ) shouldBe exception.raiseError[Try, List[SendingResult]]
    }

    "fail mapping the events " +
      "if the transform function fails on one of the events" in new TestCase {

      val level1Parent1 = commitIds.generateOne
      val level1Parent2 = commitIds.generateOne
      val level1Info    = commitInfos(startCommit.id, level1Parent1, level1Parent2).generateOne

      val level2Infos @ level2Info1 +: _ = List(
        commitInfos(level1Parent1, noParents).generateOne,
        commitInfos(level1Parent2, noParents).generateOne
      )
      givenFindingCommitInfoReturns(level1Info, level2Infos)

      val exception = exceptions.generateOne
      val failingSend: CommitEvent => Try[SendingResult] = event =>
        if (event.id == level2Info1.id) exception.raiseError[Try, SendingResult]
        else Created.pure[Try]
      source.transformEventsWith(failingSend) shouldBe exception.raiseError[Try, List[SendingResult]]
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val startCommit      = startCommits.generateOne
    val maybeAccessToken = Gen.option(accessTokens).generateOne
    val fixedNow         = Instant.now
    private val clock    = Clock.fixed(fixedNow, ZoneId.systemDefault)

    def send(returning: (CommitId, SendingResult)*): CommitEvent => Try[SendingResult] = event => {
      event.batchDate.value shouldBe fixedNow
      Try(returning.toMap.getOrElse(event.id, fail(s"Cannot find expected sending result for commitId ${event.id}")))
    }

    val commitInfoFinder      = mock[CommitInfoFinder[Try]]
    private val sourceBuilder = new CommitEventsSourceBuilder[Try](commitInfoFinder)
    val Success(source)       = sourceBuilder.buildEventsSource(startCommit, maybeAccessToken, clock)

    def givenFindingCommitInfoReturns(commitInfo: CommitInfo, otherInfos: Seq[CommitInfo]*): Unit =
      givenFindingCommitInfoReturns(commitInfo +: otherInfos.flatten: _*)

    def givenFindingCommitInfoReturns(commitInfos: CommitInfo*): Unit =
      commitInfos foreach { commitInfo =>
        (commitInfoFinder
          .findCommitInfo(_: Id, _: CommitId, _: Option[AccessToken]))
          .expects(startCommit.project.id, commitInfo.id, maybeAccessToken)
          .returning(context pure commitInfo)
      }
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

  private val someParents: Gen[List[CommitId]] = parentsIdsLists(minNumber = 1, maxNumber = 5)
}
