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

package ch.datascience.commiteventservice.events.categories.commitsync
package eventgeneration
package historytraversal

import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEvent._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.Generators._
import ch.datascience.events.consumers.Project
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events._
import eu.timepit.refined.auto._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.util._

class CommitToEventLogSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "storeCommitsInEventLog" should {

    "transform the Commit into commit events and store them in the Event Log" in new TestCase {

      val commitEvent = commitEventFrom(startCommit, project).generateOne

      (commitEventSender
        .send(_: CommitEvent))
        .expects(commitEvent)
        .returning(().pure[Try])

      commitToEventLog.storeCommitsInEventLog(project, startCommit, batchDate) shouldBe Success(
        Created
      )
    }

    "transform the Start Commit into a SkippedCommitEvent if the commit event message is 'renku migrate'" in new TestCase {

      val skippedCommit = startCommit.copy(message = CommitMessage(sentenceContaining("renku migrate").generateOne))
      val commitEvent =
        skippedCommitEventFrom(skippedCommit, project).generateOne

      (commitEventSender
        .send(_: CommitEvent))
        .expects(commitEvent)
        .returning(().pure[Try])

      commitToEventLog.storeCommitsInEventLog(project, skippedCommit, batchDate) shouldBe Success(
        Created
      )
    }

    "return a Failed status if sending the event fails" in new TestCase {

      val commitEvent = commitEventFrom(startCommit, project).generateOne

      val exception = exceptions.generateOne
      (commitEventSender
        .send(_: CommitEvent))
        .expects(commitEvent)
        .returning(exception.raiseError[Try, Unit])

      commitToEventLog.storeCommitsInEventLog(project, startCommit, batchDate) shouldBe
        Failed(failedStoring(startCommit, project), exception).pure[Try]
    }
  }

  private trait TestCase {

    val startCommit = commitInfos.generateOne
    val batchDate   = BatchDate(Instant.now)
    val project     = projects.generateOne

    val commitEventSender = mock[CommitEventSender[Try]]

    val commitToEventLog = new CommitToEventLogImpl[Try](
      commitEventSender
    )

    def failedStoring(startCommit: CommitInfo, project: Project): String =
      s"$categoryName: id = ${startCommit.id}, projectId = ${project.id}, projectPath = ${project.path} -> " +
        "storing in the event log failed"

    def commitEventFrom(commitInfo: CommitInfo, project: Project): Gen[CommitEvent] = NewCommitEvent(
      id = commitInfo.id,
      message = commitInfo.message,
      committedDate = commitInfo.committedDate,
      author = commitInfo.author,
      committer = commitInfo.committer,
      parents = commitInfo.parents,
      project = project,
      batchDate = batchDate
    )

    def skippedCommitEventFrom(commitInfo: CommitInfo, project: Project): Gen[CommitEvent] = SkippedCommitEvent(
      id = commitInfo.id,
      message = commitInfo.message,
      committedDate = commitInfo.committedDate,
      author = commitInfo.author,
      committer = commitInfo.committer,
      parents = commitInfo.parents,
      project = project,
      batchDate = batchDate
    )
  }
}
