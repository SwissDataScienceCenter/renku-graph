/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.commiteventservice.events.categories.globalcommitsync
package eventgeneration

import cats.syntax.all._
import io.renku.commiteventservice.events.categories.common.UpdateResult.{Deleted, Failed}
import io.renku.commiteventservice.events.categories.common.{CommitEventsRemover, SynchronizationSummary, UpdateResult}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.CommonGraphGenerators.personalAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, nonEmptyStrings}
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.http.client.AccessToken
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class CommitEventDeleterSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "deleteExtraneousCommits" should {

    "successfully delete commits" in new TestCase {
      val project         = consumerProjects.generateOne
      val commitsToDelete = commitIds.generateNonEmptyList().toList

      commitsToDelete foreach {
        (commitEventsRemover.removeDeletedEvent _)
          .expects(project, _)
          .returning(Deleted.pure[Try])
      }

      commitEventDeleter.deleteCommits(project, commitsToDelete) shouldBe
        SynchronizationSummary().updated(Deleted, commitsToDelete.length).pure[Try]
    }

    "return synchronization summary of failed events " +
      "if EventStatusPatcher returns Failure while sending the deletion status" in new TestCase {
        val project         = consumerProjects.generateOne
        val commitsToDelete = commitIds.generateNonEmptyList().toList

        val failure = Failed(nonEmptyStrings().generateOne, exceptions.generateOne)
        commitsToDelete foreach {
          (commitEventsRemover.removeDeletedEvent _)
            .expects(project, _)
            .returning(failure.pure[Try])
        }

        commitEventDeleter.deleteCommits(project, commitsToDelete) shouldBe
          SynchronizationSummary().updated(failure, commitsToDelete.length).pure[Try]
      }

    "return synchronization summary of failed events " +
      "if EventStatusPatcher fails while sending the deletion status" in new TestCase {
        val project         = consumerProjects.generateOne
        val commitsToDelete = commitIds.generateNonEmptyList().toList

        val exception = exceptions.generateOne
        commitsToDelete foreach {
          (commitEventsRemover.removeDeletedEvent _)
            .expects(project, _)
            .returning(exception.raiseError[Try, UpdateResult])
        }

        commitEventDeleter.deleteCommits(project, commitsToDelete) shouldBe
          SynchronizationSummary()
            .updated(Failed(s"$categoryName Failed to delete commit", exception), commitsToDelete.length)
            .pure[Try]
      }
  }

  private trait TestCase {

    implicit val maybeAccessToken: Option[AccessToken] = personalAccessTokens.generateOption

    val commitEventsRemover = mock[CommitEventsRemover[Try]]
    val commitEventDeleter  = new CommitEventDeleterImpl[Try](commitEventsRemover)
  }
}
