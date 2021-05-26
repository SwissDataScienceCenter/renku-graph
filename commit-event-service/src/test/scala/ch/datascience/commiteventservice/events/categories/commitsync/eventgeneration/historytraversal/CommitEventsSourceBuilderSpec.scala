/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration
package historytraversal

import cats.MonadError
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEvent._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.Generators._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal.EventCreationResult._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects.Id
import ch.datascience.http.client.AccessToken
import eu.timepit.refined.auto._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Instant, ZoneId, ZoneOffset}
import scala.reflect.ClassTag
import scala.util.{Success, Try}

class CommitEventsSourceBuilderSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "newCommitEventSource.transformEventsWith" should {

    "map a single commit event with the transform function " +
      "if found commit info has no parents " +
      "and the event get created in the Event Log" in new TestCase {

        val commitInfo = commitInfos(startCommit.id, noParents).generateOne
        givenFindingCommitInfoReturns(commitInfo)

        source.transformEventsWith(
          send(returning = SendExpectations[NewCommitEvent](startCommit.id, Created))
        ) shouldBe Some(Created).pure[Try]
      }

    "map a single commit event with the transform function " +
      "if found commit info has no parents " +
      "and the event already exists in the Event Log" in new TestCase {

        val commitInfo = commitInfos(startCommit.id, noParents).generateOne
        givenFindingCommitInfoReturns(commitInfo)

        source.transformEventsWith(
          send(returning = SendExpectations[NewCommitEvent](startCommit.id, Existed))
        ) shouldBe Some(Existed).pure[Try]
      }

    "map a single commit event with the transform function " +
      "if found commit info has some parents " +
      "but the event for the start event already exists in the Event Log" in new TestCase {

        val commitInfo = commitInfos(startCommit.id, someParents).generateOne
        givenFindingCommitInfoReturns(commitInfo)

        source.transformEventsWith(
          send(returning = SendExpectations[NewCommitEvent](startCommit.id, Existed))
        ) shouldBe Some(Existed).pure[Try]
      }

    "map commit events with the transform function " +
      "starting from given start commit to the oldest ancestor " +
      "skipping commits with the 'don't care' 0000000000000000000000000000000000000000 ref" in new TestCase {

        val commitId = CommitId("0000000000000000000000000000000000000000")

        source.transformEventsWith(
          send(SendExpectations[NewCommitEvent](commitId, Created))
        ) shouldBe Option.empty[EventCreationResult].pure[Try]
      }

    "map commit events with the transform function " +
      "starting from given start commit to the oldest ancestor " +
      "creating SkippedCommitEvents for commits with a message containing 'renku migrate'" in new TestCase {

        val commitMessage = CommitMessage(sentenceContaining("renku migrate").generateOne)
        val commitInfo    = commitInfos(startCommit.id).generateOne.copy(message = commitMessage)

        givenFindingCommitInfoReturns(commitInfo)

        source.transformEventsWith(
          send(SendExpectations[SkippedCommitEvent](startCommit.id, Created))
        ) shouldBe Some(Created).pure[Try]
      }

    "fail mapping the events " +
      "if finding commit info fails" in new TestCase {

        val commitInfo = commitInfos(startCommit.id, parentsIdsLists(minNumber = 2, maxNumber = 2)).generateOne

        givenFindingCommitInfoReturns(commitInfo)

        val exception = exceptions.generateOne
        (commitInfoFinder
          .findCommitInfo(_: Id, _: CommitId, _: Option[AccessToken]))
          .expects(startCommit.project.id, startCommit.id, maybeAccessToken)
          .returning(context.raiseError(exception))

        source.transformEventsWith(
          send(SendExpectations[NewCommitEvent](startCommit.id, Created))
        ) shouldBe exception.raiseError[Try, Option[EventCreationResult]]
      }

    "fail mapping the events " +
      "if the transform function fails on one of the events" in new TestCase {

        val commitInfo = commitInfos(startCommit.id).generateOne

        givenFindingCommitInfoReturns(commitInfo)

        val exception = exceptions.generateOne
        val failingSend: CommitEvent => Try[EventCreationResult] = event =>
          if (event.id == commitInfo.id) exception.raiseError[Try, EventCreationResult]
          else Created.pure[Try]
        source.transformEventsWith(failingSend) shouldBe exception.raiseError[Try, List[EventCreationResult]]
      }
  }

  private case class SendExpectations[E <: CommitEvent](
      commitId:             CommitId,
      creationResult:       EventCreationResult
  )(implicit val eventType: ClassTag[E])

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val startCommit      = startCommits.generateOne
    val maybeAccessToken = Gen.option(accessTokens).generateOne
    val fixedNow         = Instant.now
    private val clock    = Clock.fixed(fixedNow, ZoneId.of(ZoneOffset.UTC.getId))

    def send(returning: SendExpectations[_ <: CommitEvent]*): CommitEvent => Try[EventCreationResult] =
      event => {
        val sendExpectations = returning
          .find { case SendExpectations(id, _) => id == event.id }
          .getOrElse(fail(s"Cannot find expected sending result for commitId ${event.id}"))

        event.getClass        shouldBe sendExpectations.eventType.runtimeClass
        event.batchDate.value shouldBe fixedNow
        sendExpectations.creationResult.pure[Try]
      }

    val commitInfoFinder      = mock[CommitInfoFinder[Try]]
    private val sourceBuilder = new CommitEventsSourceBuilder[Try](commitInfoFinder)
    val Success(source)       = sourceBuilder.buildEventsSource(startCommit, maybeAccessToken, clock)

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
      info       <- Generators.commitInfos
      parentsIds <- parentsGenerator
    } yield info.copy(id = commitId, parents = parentsIds)

  private lazy val noParents: Gen[List[CommitId]] = Gen.const(List.empty)

  private lazy val someParents: Gen[List[CommitId]] = parentsIdsLists(minNumber = 1, maxNumber = 5)
}
