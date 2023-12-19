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

package io.renku.eventlog.events.consumers.statuschange

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.eventlog.EventLogPostgresSpec
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.events.Generators.{subscriberIds, subscriberUrls}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectIds
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class DeliveryInfoRemoverSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {

  private val projectId     = projectIds.generateOne
  private val subscriberId  = subscriberIds.generateOne
  private val subscriberUrl = subscriberUrls.generateOne
  private val sourceUrl     = microserviceBaseUrls.generateOne

  "deleteDelivery" should {

    "remove delivery info for the given eventId from the DB" in testDBResource.use { implicit cfg =>
      for {
        event <-
          storeGeneratedEvent(eventStatuses.generateOne, eventDates.generateOne, consumerProjects.generateOne)
        _ <- upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)
        _ <- upsertEventDelivery(event.eventId, subscriberId)

        otherEvent <-
          storeGeneratedEvent(eventStatuses.generateOne, eventDates.generateOne, consumerProjects.generateOne)
        _ <- upsertEventDelivery(otherEvent.eventId, subscriberId)

        _ <- findAllEventDeliveries.asserting {
               _ should contain theSameElementsAs List(FoundDelivery(event.eventId, subscriberId),
                                                       FoundDelivery(otherEvent.eventId, subscriberId)
               )
             }

        _ <- moduleSessionResource.session.useKleisli(deliveryRemover.deleteDelivery(event.eventId)).assertNoException

        _ <- findAllEventDeliveries.asserting(_ shouldBe List(FoundDelivery(otherEvent.eventId, subscriberId)))
      } yield Succeeded
    }

    "do nothing if there's no event with the given id in the DB" in testDBResource.use { implicit cfg =>
      moduleSessionResource.session
        .useKleisli(deliveryRemover.deleteDelivery(compoundEventIds.generateOne))
        .assertNoException
    }
  }

  private def deliveryRemover = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new DeliveryInfoRemoverImpl[IO]
  }
}
