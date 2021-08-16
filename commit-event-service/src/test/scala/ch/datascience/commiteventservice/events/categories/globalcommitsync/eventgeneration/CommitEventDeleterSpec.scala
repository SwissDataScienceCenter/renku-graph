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

import ch.datascience.commiteventservice.events.categories.common.EventStatusPatcher
import ch.datascience.commiteventservice.events.categories.common.UpdateResult.{Deleted, Failed}
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer.SynchronizationSummary
import ch.datascience.events.consumers.ConsumersModelGenerators.projectsGen
import ch.datascience.generators.CommonGraphGenerators.personalAccessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class CommitEventDeleterSpec extends AnyWordSpec with should.Matchers with MockFactory {
  "deleteExtraneousCommits" should {
    "Successfully delete commits" in new TestCase {
      val project         = projectsGen.generateOne
      val commitsToDelete = commitIds.generateNonEmptyList().toList

      commitsToDelete.foreach {
        (eventStatusPatcher.sendDeletionStatus _)
          .expects(project.id, _)
          .returning(Success(()))
      }

      commitEventDeleter.deleteExtraneousCommits(project, commitsToDelete)(maybeAccessToken) shouldBe Success(
        SynchronizationSummary().updated(Deleted, commitsToDelete.length)
      )
    }

    "Synchronization summary of failed events " +
      "if EventStatusPatcher sending deletion status fails" in new TestCase {
        val project         = projectsGen.generateOne
        val commitsToDelete = commitIds.generateNonEmptyList().toList
        val exception       = exceptions.generateOne

        commitsToDelete.foreach {
          (eventStatusPatcher.sendDeletionStatus _)
            .expects(project.id, _)
            .returning(Failure(exception))
        }

        commitEventDeleter.deleteExtraneousCommits(project, commitsToDelete)(maybeAccessToken) shouldBe Success(
          SynchronizationSummary().updated(Failed("Failed to delete commit", exception), commitsToDelete.length)
        )
      }
  }

  trait TestCase {

    implicit val maybeAccessToken = personalAccessTokens.generateOption

    val eventStatusPatcher = mock[EventStatusPatcher[Try]]

    def givenSendingDeletionStatusSucceeds(projectId: projects.Id, eventId: CommitId) =
      (eventStatusPatcher.sendDeletionStatus _)
        .expects(projectId, eventId)
        .returning(Success(()))

    val commitEventDeleter = new CommitEventDeleterImpl[Try](eventStatusPatcher)
  }
}
