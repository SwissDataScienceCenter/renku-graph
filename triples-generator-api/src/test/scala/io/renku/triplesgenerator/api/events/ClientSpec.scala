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

package io.renku.triplesgenerator.api.events

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import Generators._
import cats.Show
import io.circe.syntax._
import io.circe.Encoder
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.events.producers.EventSender
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues

import scala.util.Try

class ClientSpec extends AnyWordSpec with should.Matchers with MockFactory with TryValues {

  "send ProjectViewedEvent" should {

    "send the given event through the EventSender" in new TestCase {

      val event = projectViewedEvents.generateOne

      givenSending(event, ProjectViewedEvent.categoryName, returning = ().pure[Try])

      client.send(event).success.value shouldBe ()
    }
  }

  "send DatasetViewedEvent" should {

    "send the given event through the EventSender" in new TestCase {

      val event = datasetViewedEvents.generateOne

      givenSending(event, DatasetViewedEvent.categoryName, returning = ().pure[Try])

      client.send(event).success.value shouldBe ()
    }
  }

  "send ProjectViewingDeletion" should {

    "send the given event through the EventSender" in new TestCase {

      val event = projectViewingDeletions.generateOne

      givenSending(event, ProjectViewingDeletion.categoryName, returning = ().pure[Try])

      client.send(event).success.value shouldBe ()
    }
  }

  private trait TestCase {

    private val eventSender = mock[EventSender[Try]]
    val client              = new ClientImpl[Try](eventSender)

    def givenSending[E](event: E, categoryName: CategoryName, returning: Try[Unit])(implicit
        encoder: Encoder[E],
        show:    Show[E]
    ) = (eventSender
      .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
      .expects(
        EventRequestContent.NoPayload(event.asJson),
        EventSender.EventContext(categoryName, show"$categoryName: sending event $event failed")
      )
      .returning(returning)
  }
}
