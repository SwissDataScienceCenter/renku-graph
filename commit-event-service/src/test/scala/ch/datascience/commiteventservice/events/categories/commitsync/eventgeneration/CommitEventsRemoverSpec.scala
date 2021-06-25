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

import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.Generators.fullCommitSyncEvents
import ch.datascience.commiteventservice.events.categories.commitsync.categoryName
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult._
import ch.datascience.events.EventRequestContent
import ch.datascience.events.producers.EventSender
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventStatus.AwaitingDeletion
import ch.datascience.tinytypes.json.TinyTypeEncoders
import io.circe.literal._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class CommitEventsRemoverSpec extends AnyWordSpec with should.Matchers with MockFactory with TinyTypeEncoders {

  "removeDeletedEvent" should {

    "send the toAwaitingDeletion status change event" in new TestCase {

      val eventRequestContent = EventRequestContent(json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "id":           ${event.id},
        "project": {
          "id":   ${event.project.id},
          "path": ${event.project.path}
        },
        "newStatus": $AwaitingDeletion
      }""")

      (eventSender.sendEvent _)
        .expects(eventRequestContent, s"$categoryName: Marking event as $AwaitingDeletion failed")
        .returning(().pure[Try])

      commitRemover.removeDeletedEvent(event.project, event.id) shouldBe Deleted.pure[Try]
    }

    "return Failed if sending the toAwaitingDeletion status change event fails" in new TestCase {

      val exception = exceptions.generateOne
      (eventSender.sendEvent _)
        .expects(*, *)
        .returning(exception.raiseError[Try, Unit])

      commitRemover.removeDeletedEvent(event.project, event.id) shouldBe Failed(
        s"$categoryName - Commit Remover failed to send commit deletion status",
        exception
      ).pure[Try]
    }
  }

  private trait TestCase {
    val event = fullCommitSyncEvents.generateOne

    val eventSender   = mock[EventSender[Try]]
    val commitRemover = new CommitEventsRemoverImpl[Try](eventSender)
  }
}
