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

package io.renku.eventlog.subscriptions.triplesgenerated

import cats.syntax.all._
import ch.datascience.events.EventRequestContent
import ch.datascience.events.consumers.subscriptions._
import ch.datascience.events.producers.EventSender
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.tinytypes.json.TinyTypeEncoders
import io.circe.literal._
import io.renku.eventlog.EventMessage
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class DispatchRecoverySpec extends AnyWordSpec with should.Matchers with MockFactory with TinyTypeEncoders {

  "returnToQueue" should {

    s"change the status back to $TriplesGenerated" in new TestCase {

      val eventRequestContent = EventRequestContent(
        json"""
        {
          "categoryName": "EVENTS_STATUS_CHANGE",
          "id":           ${event.id.id},
          "project": {
            "id":   ${event.id.projectId},
            "path": ${event.projectPath}
          },
          "newStatus": $TriplesGenerated
        }"""
      )

      (eventSender.sendEvent _)
        .expects(eventRequestContent, s"${SubscriptionCategory.name}: Marking event as $TriplesGenerated failed")
        .returning(().pure[Try])

      dispatchRecovery.returnToQueue(event) shouldBe ().pure[Try]
    }
  }

  "recovery" should {

    "retry changing event status if status update failed initially" in new TestCase {

      val exception  = exceptions.generateOne
      val subscriber = subscriberUrls.generateOne

      val eventRequestContent = EventRequestContent(
        json"""
        {
          "categoryName": "EVENTS_STATUS_CHANGE",
          "id":           ${event.id.id},
          "project": {
            "id":   ${event.id.projectId},
            "path": ${event.projectPath}
          },
          "message": ${EventMessage(exception)},
          "newStatus": $TransformationNonRecoverableFailure
        }"""
      )

      (eventSender.sendEvent _)
        .expects(eventRequestContent,
                 s"${SubscriptionCategory.name}: $event, url = $subscriber -> $TransformationNonRecoverableFailure"
        )
        .returning(().pure[Try])

      dispatchRecovery.recover(subscriber, event)(exception) shouldBe ().pure[Try]

      logger.loggedOnly(
        Error(s"${SubscriptionCategory.name}: $event, url = $subscriber -> $TransformationNonRecoverableFailure",
              exception
        )
      )
    }
  }

  private trait TestCase {
    val event = triplesGeneratedEvents.generateOne

    val eventSender = mock[EventSender[Try]]

    val logger = TestLogger[Try]()
    val dispatchRecovery = new DispatchRecoveryImpl[Try](
      eventSender,
      logger
    )
  }
}
