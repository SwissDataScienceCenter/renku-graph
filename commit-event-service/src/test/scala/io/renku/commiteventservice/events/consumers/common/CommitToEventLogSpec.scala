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

package io.renku.commiteventservice.events.consumers.common

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.literal._
import io.renku.commiteventservice.events.consumers.commitsync.categoryName
import io.renku.commiteventservice.events.consumers.common.CommitEvent._
import io.renku.commiteventservice.events.consumers.common.Generators._
import io.renku.commiteventservice.events.consumers.common.UpdateResult._
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.events._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.util._

class CommitToEventLogSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "storeCommitsInEventLog" should {

    "transform the given commit into creation event and send it to event-log" in new TestCase {

      val event = commitEventFrom(startCommit, project).generateOne

      val serializedBody = jsons.generateOne.noSpaces
      (eventSerializer.serialiseToJsonString _).expects(event).returning(serializedBody)

      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          requestContent(event -> serializedBody),
          EventSender.EventContext(CategoryName("CREATION"), failedStoring(startCommit, event.project))
        )
        .returning(().pure[Try])

      commitToEventLog.storeCommitInEventLog(project, startCommit, batchDate) shouldBe Created.pure[Try]
    }

    "transform the Start Commit into a SkippedCommitEvent if the commit event message contains 'renku migrate'" in new TestCase {

      val migrationCommit = startCommit.copy(message = CommitMessage(sentenceContaining("renku migrate").generateOne))
      val event           = skippedCommitEventFrom(migrationCommit, project).generateOne

      val serializedBody = jsons.generateOne.noSpaces
      (eventSerializer.serialiseToJsonString _).expects(event).returning(serializedBody)

      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          requestContent(event -> serializedBody),
          EventSender.EventContext(CategoryName("CREATION"), failedStoring(startCommit, event.project))
        )
        .returning(().pure[Try])

      commitToEventLog.storeCommitInEventLog(project, migrationCommit, batchDate) shouldBe Created.pure[Try]
    }

    "return a Failed status if sending the event fails" in new TestCase {

      val event = commitEventFrom(startCommit, project).generateOne

      val exception      = exceptions.generateOne
      val serializedBody = jsons.generateOne.noSpaces
      (eventSerializer.serialiseToJsonString _).expects(event).returning(serializedBody)

      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          requestContent(event -> serializedBody),
          EventSender.EventContext(CategoryName("CREATION"), failedStoring(startCommit, event.project))
        )
        .returning(exception.raiseError[Try, Unit])

      commitToEventLog.storeCommitInEventLog(project, startCommit, batchDate) shouldBe
        Failed(failedStoring(startCommit, project), exception).pure[Try]
    }
  }

  private trait TestCase {

    val startCommit = commitInfos.generateOne
    val batchDate   = BatchDate(Instant.now)
    val project     = consumerProjects.generateOne

    val eventSender     = mock[EventSender[Try]]
    val eventSerializer = mock[CommitEventSerializer]

    val commitToEventLog = new CommitToEventLogImpl[Try](eventSender, eventSerializer)

    def failedStoring(commit: CommitInfo, project: Project): String =
      show"$categoryName: creating event id = ${commit.id}, $project in event-log failed"

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

    def requestContent(eventAndBody: (CommitEvent, EventBody)) = EventRequestContent.NoPayload(
      eventAndBody match {
        case (event: NewCommitEvent, body) => json"""{
            "categoryName": "CREATION", 
            "id":        ${event.id.value},
            "project": {
              "id":      ${event.project.id.value},
              "path":    ${event.project.path.value}
            },
            "date":      ${event.committedDate.value},
            "batchDate": ${event.batchDate.value},
            "body":      ${body.value},
            "status":    ${event.status.value}
          }"""
        case (event: SkippedCommitEvent, body) => json"""{
            "categoryName": "CREATION",
            "id":        ${event.id.value},
            "project": {
              "id":      ${event.project.id.value},
              "path":    ${event.project.path.value}
            },
            "date":      ${event.committedDate.value},
            "batchDate": ${event.batchDate.value},
            "body":      ${body.value},
            "status":    ${event.status.value},
            "message":   ${event.message.value}
          }"""
      }
    )
  }
}
