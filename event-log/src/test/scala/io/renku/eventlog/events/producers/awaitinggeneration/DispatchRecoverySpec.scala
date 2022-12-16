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

package io.renku.eventlog.events.producers
package awaitinggeneration

import Generators.sendingResults
import cats.syntax.all._
import io.circe.literal._
import io.renku.events.consumers.subscriptions._
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.events.EventMessage
import io.renku.graph.model.events.EventStatus._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class DispatchRecoverySpec extends AnyWordSpec with should.Matchers with MockFactory {

  "returnToQueue" should {

    s"change the status back to $New" in new TestCase {

      val eventRequestContent = EventRequestContent.NoPayload(json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "id":           ${event.id.id},
        "project": {
          "id":   ${event.id.projectId},
          "path": ${event.projectPath}
        },
        "newStatus": $New
      }""")

      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          eventRequestContent,
          EventSender.EventContext(CategoryName("EVENTS_STATUS_CHANGE"),
                                   s"${SubscriptionCategory.categoryName}: Marking event as $New failed"
          )
        )
        .returning(().pure[Try])

      dispatchRecovery.returnToQueue(event, sendingResults.generateOne) shouldBe ().pure[Try]
    }
  }

  "recovery" should {

    "retry changing event status if status update failed initially" in new TestCase {

      val exception  = exceptions.generateOne
      val subscriber = subscriberUrls.generateOne

      val eventRequestContent = EventRequestContent.NoPayload(json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "id":           ${event.id.id},
        "project": {
          "id":   ${event.id.projectId},
          "path": ${event.projectPath}
        },
        "message": ${EventMessage(exception)},
        "newStatus": $GenerationNonRecoverableFailure
      }""")

      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          eventRequestContent,
          EventSender.EventContext(
            CategoryName("EVENTS_STATUS_CHANGE"),
            s"${SubscriptionCategory.categoryName}: $event, url = $subscriber -> $GenerationNonRecoverableFailure"
          )
        )
        .returning(().pure[Try])

      dispatchRecovery.recover(subscriber, event)(exception) shouldBe ().pure[Try]

      logger.loggedOnly(
        Error(s"${SubscriptionCategory.categoryName}: $event, url = $subscriber -> $GenerationNonRecoverableFailure",
              exception
        )
      )
    }
  }

  private trait TestCase {
    val event = awaitingGenerationEvents.generateOne

    val eventSender = mock[EventSender[Try]]
    implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val dispatchRecovery = new DispatchRecoveryImpl[Try](eventSender)
  }
}
