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

package io.renku.eventlog.subscriptions.eventdelivery

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.eventlog.subscriptions.TestCompoundIdEvent
import io.renku.eventlog.subscriptions.TestCompoundIdEvent.testCompoundIdEvent
import io.renku.events.consumers.subscriptions._
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.CompoundEventId
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventDeliverySpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with should.Matchers {

  "registerSending" should {

    "add association between the given event and subscriber id " +
      "if it does not exist" in new TestCase {

        addEvent(event.compoundEventId)
        upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)
        upsertSubscriber(subscriberId, subscriberUrl, sourceUrl = microserviceBaseUrls.generateOne)

        findAllEventDeliveries shouldBe Nil

        delivery.registerSending(event, subscriberUrl).unsafeRunSync() shouldBe ()

        findAllEventDeliveries shouldBe List(event.compoundEventId -> subscriberId)

        val otherEvent = testCompoundIdEvent.generateOne
        addEvent(otherEvent.compoundEventId)

        delivery.registerSending(otherEvent, subscriberUrl).unsafeRunSync() shouldBe ()

        findAllEventDeliveries.toSet shouldBe Set(
          event.compoundEventId      -> subscriberId,
          otherEvent.compoundEventId -> subscriberId
        )
      }

    "replace the delivery_id if the association between the given event and subscriber url already exists" in new TestCase {

      addEvent(event.compoundEventId)
      upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)

      delivery.registerSending(event, subscriberUrl).unsafeRunSync() shouldBe ()

      findAllEventDeliveries shouldBe List(event.compoundEventId -> subscriberId)

      val newSubscriberId = subscriberIds.generateOne
      upsertSubscriber(newSubscriberId, subscriberUrl, sourceUrl)

      delivery.registerSending(event, subscriberUrl).unsafeRunSync() shouldBe ()
      findAllEventDeliveries shouldBe List(event.compoundEventId -> newSubscriberId)
    }

    "update the delivery_id if the id is a DeletingProjectDeliverId" in new DeletingProjectTestCase {
      addEvent(event.compoundEventId)
      upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)

      delivery.registerSending(event, subscriberUrl).unsafeRunSync() shouldBe ()

      findAllProjectDeliveries shouldBe List((event.compoundEventId.projectId, subscriberId, DeletingProjectTypeId))
    }

    "do nothing if the same event is delivered twice with a DeletingProjectTypeId" in new DeletingProjectTestCase {
      addEvent(event.compoundEventId)
      upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)

      delivery.registerSending(event, subscriberUrl).unsafeRunSync() shouldBe ()

      findAllProjectDeliveries shouldBe List((event.compoundEventId.projectId, subscriberId, DeletingProjectTypeId))

      delivery.registerSending(event, subscriberUrl).unsafeRunSync() shouldBe ()

      findAllProjectDeliveries shouldBe List((event.compoundEventId.projectId, subscriberId, DeletingProjectTypeId))
    }
  }

  private trait CommonTestCase {
    val event            = testCompoundIdEvent.generateOne
    val subscriberId     = subscriberIds.generateOne
    val subscriberUrl    = subscriberUrls.generateOne
    val sourceUrl        = microserviceBaseUrls.generateOne
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
  }

  private trait TestCase extends CommonTestCase {

    val compoundIdExtractor: TestCompoundIdEvent => CompoundEventDeliveryId = e =>
      CompoundEventDeliveryId(e.compoundEventId)
    val delivery = new EventDeliveryImpl[IO, TestCompoundIdEvent](compoundIdExtractor, queriesExecTimes, sourceUrl)
  }

  private trait DeletingProjectTestCase extends CommonTestCase {

    val compoundIdExtractor: TestCompoundIdEvent => EventDeliveryId = event => DeletingProjectDeliverId(event.projectId)
    val delivery = new EventDeliveryImpl[IO, TestCompoundIdEvent](compoundIdExtractor, queriesExecTimes, sourceUrl)
  }

  private def addEvent(eventId: CompoundEventId): Unit = storeEvent(eventId,
                                                                    eventStatuses.generateOne,
                                                                    executionDates.generateOne,
                                                                    eventDates.generateOne,
                                                                    eventBodies.generateOne
  )
}
