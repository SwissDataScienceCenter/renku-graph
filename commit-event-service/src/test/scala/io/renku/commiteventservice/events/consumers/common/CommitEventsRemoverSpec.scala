/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.commiteventservice.events.consumers.common

import cats.syntax.all._
import io.circe.literal._
import io.renku.commiteventservice.events.consumers.commitsync.categoryName
import io.renku.commiteventservice.events.consumers.common.UpdateResult.{Deleted, Failed}
import io.renku.events.consumers.Project
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.events.EventStatus.AwaitingDeletion
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class CommitEventsRemoverSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "removeDeletedEvent" should {

    "send the toAwaitingDeletion status change event" in new TestCase {

      val eventRequestContent = EventRequestContent.NoPayload(json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "id":           $commitId,
        "project": {
          "id":   ${project.id},
          "path": ${project.path}
        },
        "subCategory": "ToAwaitingDeletion"
      }""")

      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(eventRequestContent,
                 EventSender.EventContext(CategoryName("EVENTS_STATUS_CHANGE"),
                                          s"$categoryName: Marking event as $AwaitingDeletion failed"
                 )
        )
        .returning(().pure[Try])

      commitRemover.removeDeletedEvent(project, commitId) shouldBe Deleted.pure[Try]
    }

    "return Failed if sending the toAwaitingDeletion status change event fails" in new TestCase {

      val exception = exceptions.generateOne
      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(*, *)
        .returning(exception.raiseError[Try, Unit])

      commitRemover.removeDeletedEvent(project, commitId) shouldBe Failed(
        s"$categoryName: Commit Remover failed to send commit deletion status",
        exception
      ).pure[Try]
    }
  }

  private trait TestCase {
    val project = {
      for {
        projectId   <- projectIds
        projectPath <- projectPaths
      } yield Project(projectId, projectPath)
    }.generateOne
    val commitId = commitIds.generateOne

    val eventSender   = mock[EventSender[Try]]
    val commitRemover = new CommitEventsRemoverImpl[Try](eventSender)
  }
}
