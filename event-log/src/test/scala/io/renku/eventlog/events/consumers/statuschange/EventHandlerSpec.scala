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

package io.renku.eventlog.events.consumers
package statuschange

import cats.effect.IO
import cats.syntax.all._
import io.circe.syntax._
import io.renku.eventlog.events.consumers.statuschange.StatusChangeEvent._
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.EventRequestContent
import io.renku.events.consumers.ProcessExecutor
import io.renku.events.producers.EventSender
import io.renku.interpreters.TestLogger
import io.renku.metrics.{MetricsRegistry, TestMetricsRegistry}
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EventHandlerSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with EitherValues
    with should.Matchers
    with ScalaCheckPropertyChecks {

  def eventRequestContent(event: StatusChangeEvent): EventRequestContent = event match {
    case e: StatusChangeEvent.ToTriplesGenerated =>
      EventRequestContent.WithPayload(event.asJson, e.payload)
    case _ => EventRequestContent.NoPayload(event.asJson)
  }

  "createHandlingDefinition.decode" should {

    "decode valid event data" in new TestCase {
      val definition = handler.createHandlingDefinition()
      forAll(StatusChangeGenerators.statusChangeEvents) { event =>
        val decoded = definition.decode(eventRequestContent(event))
        decoded shouldBe Right(event)
      }
    }

    "fail if no payload exist for TriplesGenerated" in new TestCase {
      val definition = handler.createHandlingDefinition()
      forAll(StatusChangeGenerators.toTriplesGeneratedEvents) { event =>
        val req = EventRequestContent(event.asJson)
        definition.decode(req).left.value.getMessage should endWith(show"Missing event payload for: $event")
      }
    }
  }

  "createHandlingDefinition.process" should {
    "call to StatusChanger" in new TestCase {
      val definition = handler.createHandlingDefinition()
      forAll(StatusChangeGenerators.statusChangeEvents) { event =>
        (statusChanger
          .updateStatuses(_: DBUpdater[IO, StatusChangeEvent])(_: StatusChangeEvent))
          .expects(*, event)
          .returning(IO.unit)

        definition.process(event).unsafeRunSync() shouldBe ()
      }
    }
  }

  "createHandlingDefinition" should {
    "not define onRelease and precondition" in new TestCase {
      val definition = handler.createHandlingDefinition()
      definition.precondition.unsafeRunSync() shouldBe None
      definition.onRelease                    shouldBe None
    }
  }

  private trait TestCase {

    implicit val logger:                   TestLogger[IO]            = TestLogger[IO]()
    private implicit val metricsRegistry:  MetricsRegistry[IO]       = TestMetricsRegistry[IO]
    private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    val statusChanger               = mock[StatusChanger[IO]]
    private val deliveryInfoRemover = mock[DeliveryInfoRemover[IO]]
    private val eventsQueue         = mock[StatusChangeEventsQueue[IO]]
    private val eventSender         = mock[EventSender[IO]]

    val handler =
      new EventHandler[IO](ProcessExecutor.sequential, statusChanger, eventSender, eventsQueue, deliveryInfoRemover)
  }
}
