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

package io.renku.eventlog.events.producers
package cleanup

import Generators._
import cats.effect._
import cats.syntax.all._
import io.circe.literal._
import io.renku.eventlog.events.producers.Generators.sendingResults
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.events.Generators._
import io.renku.events.producers.EventSender
import io.renku.generators.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.events.EventStatus._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DispatchRecoverySpec extends AnyWordSpec with IOSpec with should.Matchers with MockFactory {

  "returnToQueue" should {

    s"change the status back to $AwaitingDeletion" in new TestCase {

      val eventRequestContent = EventRequestContent.NoPayload(json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "project": {
          "id":   ${event.project.id},
          "path": ${event.project.path}
        },
        "subCategory": "RollbackToAwaitingDeletion"
      }""")

      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          eventRequestContent,
          EventSender.EventContext(
            CategoryName("EVENTS_STATUS_CHANGE"),
            show"$categoryName: Marking events for project: ${event.project.path} as $AwaitingDeletion failed"
          )
        )
        .returning(().pure[IO])

      dispatchRecovery.returnToQueue(event, sendingResults.generateOne).unsafeRunSync() shouldBe ()
    }
  }

  "recovery" should {

    "reIO changing event status if status update failed initially" in new TestCase {

      val exception  = exceptions.generateOne
      val subscriber = subscriberUrls.generateOne

      val eventRequestContent = EventRequestContent.NoPayload(json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "project": {
          "id":   ${event.project.id},
          "path": ${event.project.path}
        },
        "subCategory": "RollbackToAwaitingDeletion"
      }""")

      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          eventRequestContent,
          EventSender.EventContext(
            CategoryName("EVENTS_STATUS_CHANGE"),
            show"$categoryName: Marking events for project: ${event.project.path} as $AwaitingDeletion failed"
          )
        )
        .returning(().pure[IO])

      dispatchRecovery.recover(subscriber, event)(exception).unsafeRunSync() shouldBe ()

      logger.loggedOnly(Error(show"$categoryName: $event, url = $subscriber -> $AwaitingDeletion", exception))
    }
  }

  private trait TestCase {
    val event = cleanupEvents.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val eventSender      = mock[EventSender[IO]]
    val dispatchRecovery = new DispatchRecoveryImpl[IO](eventSender)
  }
}
