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

import ch.datascience.commiteventservice.events.EventStatusPatcher
import ch.datascience.commiteventservice.events.categories.commitsync.Generators.fullCommitSyncEvents
import ch.datascience.commiteventservice.events.categories.commitsync.categoryName
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult.{Deleted, Failed}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class CommitEventsRemoverSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "removeDeletedEvent" should {
    "return Deleted if marking the event for deletion succeeds" in new TestCase {
      val event = fullCommitSyncEvents.generateOne
      (eventPatcher.sendDeletionStatus _).expects(event.project.id, event.id).returning(Success(()))
      commitRemover.removeDeletedEvent(event.project, event.id) shouldBe Success(Deleted)
    }

    "return Failed if marking the event for deletion fails" in new TestCase {
      val event     = fullCommitSyncEvents.generateOne
      val exception = exceptions.generateOne

      (eventPatcher.sendDeletionStatus _)
        .expects(event.project.id, event.id)
        .returning(Failure(exception))
      commitRemover.removeDeletedEvent(event.project, event.id) shouldBe Success(
        Failed(s"$categoryName - Commit Remover failed to send commit deletion status", exception)
      )
    }
  }

  private trait TestCase {
    val eventPatcher  = mock[EventStatusPatcher[Try]]
    val commitRemover = new CommitEventsRemoverImpl[Try](eventPatcher)
  }
}
