/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.commiteventservice.events.consumers.globalcommitsync.eventgeneration

import cats.syntax.all._
import io.renku.commiteventservice.events.consumers.common.Generators.{commitInfos => commitInfosGen}
import io.renku.commiteventservice.events.consumers.common.UpdateResult.{Created, Skipped}
import io.renku.commiteventservice.events.consumers.common._
import io.renku.commiteventservice.events.consumers.globalcommitsync.Generators._
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators.personalAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.EventsGenerators.{batchDates, commitIds}
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, ZoneId, ZoneOffset}
import scala.util.{Failure, Success, Try}

class MissingCommitEventCreatorSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "createMissingCommits" should {

    "create new events and report back the summary" in new TestCase {
      val event          = globalCommitSyncEvents().generateOne
      val newCommitsInGL = commitIds.generateNonEmptyList().toList
      val commitInfos    = getCommitInfosForCommits(newCommitsInGL)

      givenCommitInfosFound(event.project.id, commitInfos)

      givenStoringCommitSucceedsWithCreated(event.project, commitInfos)

      missingCommitEventCreator
        .createCommits(event.project, newCommitsInGL)(maybeAccessToken) shouldBe SynchronizationSummary()
        .updated(Created, newCommitsInGL.length)
        .pure[Try]
    }

    "return summary with new events and one event skipped" in new TestCase {
      val event          = globalCommitSyncEvents().generateOne
      val newCommitsInGL = commitIds.generateNonEmptyList(min = 2).toList
      val commitInfos    = getCommitInfosForCommits(newCommitsInGL)

      givenCommitInfosFound(event.project.id, commitInfos)

      givenStoringCommitSucceedsWithCreated(event.project, commitInfos.tail)

      (commitToEventLog.storeCommitInEventLog _) // skip head
        .expects(event.project, commitInfos.head, batchDate)
        .returning(Success(Skipped))

      missingCommitEventCreator.createCommits(event.project, newCommitsInGL)(
        maybeAccessToken
      ) shouldBe SynchronizationSummary().updated(Created, newCommitsInGL.length - 1).updated(Skipped, 1).pure[Try]
    }

    "fail if sending commit info to Event Log fails" in new TestCase {
      val event          = globalCommitSyncEvents().generateOne
      val newCommitsInGL = commitIds.generateNonEmptyList().toList
      val commitInfos    = getCommitInfosForCommits(newCommitsInGL)

      givenCommitInfosFound(event.project.id, commitInfos)

      val exception = exceptions.generateOne
      commitInfos.foreach { commitInfo =>
        (commitToEventLog.storeCommitInEventLog _)
          .expects(event.project, commitInfo, batchDate)
          .returning(Failure(exception))
      }

      missingCommitEventCreator.createCommits(event.project, newCommitsInGL)(
        maybeAccessToken
      ) shouldBe Failure(exception)
    }
  }

  private trait TestCase {

    implicit val maybeAccessToken: Option[AccessToken] = personalAccessTokens.generateOption
    val batchDate = batchDates.generateOne

    val commitInfoFinder          = mock[CommitInfoFinder[Try]]
    val commitToEventLog          = mock[CommitToEventLog[Try]]
    val clock                     = Clock.fixed(batchDate.value, ZoneId.of(ZoneOffset.UTC.getId))
    val missingCommitEventCreator = new MissingCommitEventCreatorImpl[Try](commitInfoFinder, commitToEventLog, clock)

    def getCommitInfosForCommits(commitIds: List[CommitId]): List[CommitInfo] = commitIds.map { commitId =>
      commitInfosGen.generateOne.copy(id = commitId)
    }

    def givenCommitInfosFound(projectId: projects.GitLabId, commitInfos: List[CommitInfo]) =
      commitInfos map { commitInfo =>
        (commitInfoFinder
          .findCommitInfo(_: projects.GitLabId, _: CommitId)(_: Option[AccessToken]))
          .expects(projectId, commitInfo.id, maybeAccessToken)
          .returning(Success(commitInfo))
      }

    def givenStoringCommitSucceedsWithCreated(project: Project, commitInfos: List[CommitInfo]): Unit =
      commitInfos foreach { commitInfo =>
        (commitToEventLog.storeCommitInEventLog _)
          .expects(project, commitInfo, batchDate)
          .returning(Success(Created))
      }
  }
}
