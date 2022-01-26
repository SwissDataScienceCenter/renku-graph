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

package io.renku.eventlog.subscriptions.cleanup

import io.renku.eventlog.EventMessage
import io.renku.events.EventRequestContent
import io.renku.generators.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.events.producers.EventSender
import io.renku.events.consumers.subscriptions._
import io.renku.eventlog.subscriptions.cleanup.DispatchRecoveryImpl
import io.renku.graph.model.events.EventStatus._
import io.renku.interpreters.TestLogger
import io.renku.tinytypes.json.TinyTypeEncoders
import cats.syntax.all._
import org.scalatest.wordspec.AnyWordSpec
import scala.util.Try
import io.renku.interpreters.TestLogger.Level.Error
import io.circe.literal._
import org.scalatest.matchers.should
import org.scalamock.scalatest.MockFactory

class DispatchRecoverySpec extends AnyWordSpec with should.Matchers with MockFactory with TinyTypeEncoders {

  "returnToQueue" should {

    s"change the status back to $AwaitingDeletion" in new TestCase {

      val eventRequestContent = EventRequestContent.NoPayload(json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "project": {
          "id":   ${event.project.id},
          "path": ${event.project.path}
        },
        "newStatus": $AwaitingDeletion
      }""")

      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: String))
        .expects(eventRequestContent, s"${SubscriptionCategory.name}: Marking event as $AwaitingDeletion failed")
        .returning(().pure[Try])

      dispatchRecovery.returnToQueue(event) shouldBe ().pure[Try]
    }
  }

  "recovery" should {

    "retry changing event status if status update failed initially" in new TestCase {

      val exception  = exceptions.generateOne
      val subscriber = subscriberUrls.generateOne

      val eventRequestContent = EventRequestContent.NoPayload(json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "project": {
          "id":   ${event.project.id},
          "path": ${event.project.path}
        },
        "message": ${EventMessage(exception)},
        "newStatus": $AwaitingDeletion
      }""")

      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: String))
        .expects(
          eventRequestContent,
          s"${SubscriptionCategory.name}: $event, url = $subscriber -> $AwaitingDeletion"
        )
        .returning(().pure[Try])

      dispatchRecovery.recover(subscriber, event)(exception) shouldBe ().pure[Try]

      logger.loggedOnly(
        Error(s"${SubscriptionCategory.name}: $event, url = $subscriber -> $AwaitingDeletion", exception)
      )
    }
  }

  private trait TestCase {
    val event = cleanupEvents.generateOne

    val eventSender      = mock[EventSender[Try]]
    implicit val logger  = TestLogger[Try]()
    val dispatchRecovery = new DispatchRecoveryImpl[Try](eventSender)
  }
}
