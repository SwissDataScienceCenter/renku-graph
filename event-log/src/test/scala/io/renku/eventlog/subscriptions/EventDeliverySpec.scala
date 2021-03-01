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
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.metrics.TestLabeledHistogram
import io.renku.eventlog.EventContentGenerators._
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.InMemoryEventLogDbSpec
import ch.datascience.graph.model.EventsGenerators._
import io.renku.eventlog.subscriptions.Generators.subscriberUrls
import io.renku.eventlog.subscriptions.TestCompoundIdEvent.testCompoundIdEvent
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventDeliverySpec extends AnyWordSpec with InMemoryEventLogDbSpec with MockFactory with should.Matchers {

  "registerSending" should {

    "add association between the given event and subscriber url " +
      "if it does not exist" in new TestCase {

        addEvent(event.compoundEventId)

        findAllAssociations shouldBe Nil

        delivery.registerSending(event, subscriberUrl).unsafeRunSync() shouldBe ()

        findAllAssociations shouldBe List(event.compoundEventId -> subscriberUrl)

        val otherEvent = testCompoundIdEvent.generateOne

        addEvent(otherEvent.compoundEventId)

        delivery.registerSending(otherEvent, subscriberUrl).unsafeRunSync() shouldBe ()

        findAllAssociations.toSet shouldBe Set(
          event.compoundEventId      -> subscriberUrl,
          otherEvent.compoundEventId -> subscriberUrl
        )
      }

    "do nothing if the association between the given event and subscriber url already exists" in new TestCase {

      addEvent(event.compoundEventId)

      delivery.registerSending(event, subscriberUrl).unsafeRunSync() shouldBe ()
      delivery.registerSending(event, subscriberUrl).unsafeRunSync() shouldBe ()

      findAllAssociations shouldBe List(event.compoundEventId -> subscriberUrl)
    }
  }

  private trait TestCase {

    val event         = testCompoundIdEvent.generateOne
    val subscriberUrl = subscriberUrls.generateOne

    val compoundIdExtractor: TestCompoundIdEvent => CompoundEventId = _.compoundEventId
    val queriesExecTimes = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val delivery         = new EventDeliveryImpl[TestCompoundIdEvent](transactor, compoundIdExtractor, queriesExecTimes)
  }

  private def findAllAssociations: List[(CompoundEventId, SubscriberUrl)] = execute {
    sql"""|SELECT event_id, project_id, delivery_url
          |FROM event_delivery""".stripMargin
      .query[(CompoundEventId, SubscriberUrl)]
      .to[List]
  }

  private def addEvent(eventId: CompoundEventId): Unit = storeEvent(eventId,
                                                                    eventStatuses.generateOne,
                                                                    executionDates.generateOne,
                                                                    eventDates.generateOne,
                                                                    eventBodies.generateOne
  )
}
