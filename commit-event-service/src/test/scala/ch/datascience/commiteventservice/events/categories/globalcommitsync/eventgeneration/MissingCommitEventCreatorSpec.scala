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

package ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration
import ch.datascience.commiteventservice.events.categories.common.Generators.{commitInfos => commitInfosGen}
import ch.datascience.commiteventservice.events.categories.common.UpdateResult.{Created, Skipped}
import ch.datascience.commiteventservice.events.categories.common.{CommitInfo, CommitInfoFinder, CommitToEventLog}
import ch.datascience.commiteventservice.events.categories.globalcommitsync.Generators._
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer.SynchronizationSummary
import ch.datascience.events.consumers.Project
import ch.datascience.generators.CommonGraphGenerators.personalAccessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.EventsGenerators.{batchDates, commitIds}
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, ZoneId, ZoneOffset}
import scala.util.{Failure, Success, Try}

class MissingCommitEventCreatorSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "createMissingCommits" should {
    "Create new events and report back the summary" in new TestCase {
      val event          = globalCommitSyncEvents().generateOne
      val newCommitsInGL = commitIds.generateNonEmptyList().toList
      val commitsInGL    = newCommitsInGL ++ event.commits
      val commitInfos: List[CommitInfo] = getCommitInfosForCommits(newCommitsInGL)

      givenCommitInfosFound(event.project.id, commitInfos)

      givenStoringCommitSucceedsWithCreated(event.project, commitInfos)

      val expectedSummary = SynchronizationSummary().updated(Created, newCommitsInGL.length)

      val actualSummary: Try[SynchronizationSummary] = missingCommitEventCreator
        .createMissingCommits(event.project, newCommitsInGL)(maybeAccessToken)

      actualSummary shouldBe Success(expectedSummary)
    }

    "There are new events and one event is skipped" in new TestCase {
      val event          = globalCommitSyncEvents().generateOne
      val newCommitsInGL = commitIds.generateNonEmptyList(minElements = Refined.unsafeApply(2)).toList
      val commitsInGL    = newCommitsInGL ++ event.commits
      val commitInfos: List[CommitInfo] = getCommitInfosForCommits(newCommitsInGL)

      givenCommitInfosFound(event.project.id, commitInfos)

      givenStoringCommitSucceedsWithCreated(event.project, commitInfos.tail)

      (commitToEventLog.storeCommitInEventLog _) //skip head
        .expects(event.project, commitInfos.head, batchDate)
        .returning(Success(Skipped))

      val expectedSummary = SynchronizationSummary().updated(Created, newCommitsInGL.length - 1).updated(Skipped, 1)

      missingCommitEventCreator.createMissingCommits(event.project, newCommitsInGL)(
        maybeAccessToken
      ) shouldBe Success(expectedSummary)
    }

    "Fail if sending commit info to Event Log fails" in new TestCase {
      val event          = globalCommitSyncEvents().generateOne
      val newCommitsInGL = commitIds.generateNonEmptyList().toList
      val commitsInGL    = newCommitsInGL ++ event.commits
      val newCommitCount: Int Refined Positive = Refined.unsafeApply(newCommitsInGL.length)
      val commitInfos:    List[CommitInfo]     = getCommitInfosForCommits(newCommitsInGL)
      val exception = exceptions.generateOne

      givenCommitInfosFound(event.project.id, commitInfos)

      commitInfos.foreach { commitInfo =>
        (commitToEventLog.storeCommitInEventLog _)
          .expects(event.project, commitInfo, batchDate)
          .returning(Failure(exception))
      }

//      val expectedSummary = SynchronizationSummary().updated(Created, newCommitsInGL.length)

      missingCommitEventCreator.createMissingCommits(event.project, newCommitsInGL)(
        maybeAccessToken
      ) shouldBe Failure(exception)

    }
  }

  trait TestCase {

    val logger = TestLogger[Try]()

    implicit val maybeAccessToken = personalAccessTokens.generateOption

    val batchDate = batchDates.generateOne

    val clock = Clock.fixed(batchDate.value, ZoneId.of(ZoneOffset.UTC.getId))

    val commitInfoFinder = mock[CommitInfoFinder[Try]]

    val commitToEventLog = mock[CommitToEventLog[Try]]

    val missingCommitEventCreator = new MissingCommitEventCreatorImpl[Try](commitInfoFinder, commitToEventLog, clock)

    def getCommitInfosForCommits(commitIds: List[CommitId]): List[CommitInfo] = commitIds.map { commitId =>
      commitInfosGen.generateOne.copy(id = commitId)
    }

    def givenCommitInfosFound(projectId: projects.Id, commitInfos: List[CommitInfo]) =
      commitInfos map { commitInfo =>
        (commitInfoFinder
          .findCommitInfo(_: projects.Id, _: CommitId)(_: Option[AccessToken]))
          .expects(projectId, commitInfo.id, maybeAccessToken)
          .returning(Success(commitInfo))
      }

    def givenStoringCommitSucceedsWithCreated(project: Project, commitInfos: List[CommitInfo]) = commitInfos.foreach {
      commitInfo =>
        (commitToEventLog.storeCommitInEventLog _)
          .expects(project, commitInfo, batchDate)
          .returning(Success(Created))
    }

  }
}
