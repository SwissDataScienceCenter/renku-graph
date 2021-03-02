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

package io.renku.eventlog.subscriptions

import ch.datascience.db.SqlQuery
import ch.datascience.events.consumers.subscriptions._
import ch.datascience.generators.CommonGraphGenerators.microserviceBaseUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.metrics.TestLabeledHistogram
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.eventlog.subscriptions.TestCompoundIdEvent.testCompoundIdEvent
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventDeliverySpec extends AnyWordSpec with InMemoryEventLogDbSpec with MockFactory with should.Matchers {

  "registerSending" should {

    "add association between the given event and subscriber id " +
      "if it does not exist" in new TestCase {

        addEvent(event.compoundEventId)
        upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)
        upsertSubscriber(subscriberId, subscriberUrl, sourceUrl = microserviceBaseUrls.generateOne)

        findAllAssociations shouldBe Nil

        delivery.registerSending(event, subscriberUrl).unsafeRunSync() shouldBe ()

        findAllAssociations shouldBe List(event.compoundEventId -> subscriberId)

        val otherEvent = testCompoundIdEvent.generateOne

        addEvent(otherEvent.compoundEventId)

        delivery.registerSending(otherEvent, subscriberUrl).unsafeRunSync() shouldBe ()

        findAllAssociations.toSet shouldBe Set(
          event.compoundEventId      -> subscriberId,
          otherEvent.compoundEventId -> subscriberId
        )
      }

    "do nothing if the association between the given event and subscriber url already exists" in new TestCase {

      addEvent(event.compoundEventId)
      upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)

      delivery.registerSending(event, subscriberUrl).unsafeRunSync() shouldBe ()
      delivery.registerSending(event, subscriberUrl).unsafeRunSync() shouldBe ()

      findAllAssociations shouldBe List(event.compoundEventId -> subscriberId)
    }
  }

  private trait TestCase {

    val event         = testCompoundIdEvent.generateOne
    val subscriberId  = subscriberIds.generateOne
    val subscriberUrl = subscriberUrls.generateOne
    val sourceUrl     = microserviceBaseUrls.generateOne

    val compoundIdExtractor: TestCompoundIdEvent => CompoundEventId = _.compoundEventId
    val queriesExecTimes = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val delivery =
      new EventDeliveryImpl[TestCompoundIdEvent](transactor, compoundIdExtractor, queriesExecTimes, sourceUrl)
  }

  private def findAllAssociations: List[(CompoundEventId, SubscriberId)] = execute {
    sql"""|SELECT event_id, project_id, delivery_id
          |FROM event_delivery""".stripMargin
      .query[(CompoundEventId, SubscriberId)]
      .to[List]
  }

  private def addEvent(eventId: CompoundEventId): Unit = storeEvent(eventId,
                                                                    eventStatuses.generateOne,
                                                                    executionDates.generateOne,
                                                                    eventDates.generateOne,
                                                                    eventBodies.generateOne
  )
}
