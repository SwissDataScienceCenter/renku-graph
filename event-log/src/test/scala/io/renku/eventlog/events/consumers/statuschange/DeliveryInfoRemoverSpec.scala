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

package io.renku.eventlog.events.consumers.statuschange

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.events.consumers.subscriptions.{subscriberIds, subscriberUrls}
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.events.CompoundEventId
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DeliveryInfoRemoverSpec extends AnyWordSpec with IOSpec with should.Matchers with InMemoryEventLogDbSpec {

  "deleteDelivery" should {

    "remove delivery info for the given eventId from the DB" in new TestCase {
      val event =
        storeGeneratedEvent(eventStatuses.generateOne, eventDates.generateOne, projectId, projectPaths.generateOne)
      val eventId = CompoundEventId(event._1, projectId)
      upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)
      upsertEventDelivery(eventId, subscriberId)

      val otherEvent =
        storeGeneratedEvent(eventStatuses.generateOne, eventDates.generateOne, projectId, projectPaths.generateOne)
      val otherEventId = CompoundEventId(otherEvent._1, projectId)
      upsertEventDelivery(otherEventId, subscriberId)

      findAllEventDeliveries should contain theSameElementsAs List(eventId -> subscriberId,
                                                                   otherEventId -> subscriberId
      )

      execute(deliveryRemover.deleteDelivery(eventId)) shouldBe ()

      findAllEventDeliveries shouldBe List(otherEventId -> subscriberId)
    }

    "do nothing if there's no event with the given id in the DB" in new TestCase {
      execute(deliveryRemover.deleteDelivery(compoundEventIds.generateOne)) shouldBe ()
    }
  }

  private trait TestCase {
    val projectId     = projectIds.generateOne
    val subscriberId  = subscriberIds.generateOne
    val subscriberUrl = subscriberUrls.generateOne
    val sourceUrl     = microserviceBaseUrls.generateOne

    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val deliveryRemover  = new DeliveryInfoRemoverImpl[IO](queriesExecTimes)
  }
}
