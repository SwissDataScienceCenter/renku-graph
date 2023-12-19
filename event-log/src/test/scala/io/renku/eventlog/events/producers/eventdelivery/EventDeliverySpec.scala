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
package eventdelivery

import TestCompoundIdEvent.testCompoundIdEvent
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.Generators._
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.CompoundEventId
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class EventDeliverySpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {

  private val event         = testCompoundIdEvent.generateOne
  private val subscriberId  = subscriberIds.generateOne
  private val subscriberUrl = subscriberUrls.generateOne
  private val sourceUrl     = microserviceBaseUrls.generateOne

  it should "add association between the given event and subscriber id " +
    "if it does not exist" in testDBResource.use { implicit cfg =>
      for {
        _ <- addEvent(event.compoundEventId)

        _ <- upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)
        _ <- upsertSubscriber(subscriberId, subscriberUrl, sourceUrl = microserviceBaseUrls.generateOne)

        _ <- findAllEventDeliveries.asserting(_ shouldBe Nil)

        _ <- delivery.registerSending(event, subscriberUrl).assertNoException

        _ <- findAllEventDeliveries.asserting(_ shouldBe List(FoundDelivery(event.compoundEventId, subscriberId)))

        otherEvent = testCompoundIdEvent.generateOne
        _ <- addEvent(otherEvent.compoundEventId)

        _ <- delivery.registerSending(otherEvent, subscriberUrl).assertNoException

        _ <- findAllEventDeliveries.asserting(
               _.toSet shouldBe Set(FoundDelivery(event.compoundEventId, subscriberId),
                                    FoundDelivery(otherEvent.compoundEventId, subscriberId)
               )
             )
      } yield Succeeded
    }

  it should "replace the delivery_id if the association between the given event and subscriber url already exists" in testDBResource
    .use { implicit cfg =>
      for {
        _ <- addEvent(event.compoundEventId)
        _ <- upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)

        _ <- delivery.registerSending(event, subscriberUrl).assertNoException

        _ <- findAllEventDeliveries.asserting(_ shouldBe List(FoundDelivery(event.compoundEventId, subscriberId)))

        newSubscriberId = subscriberIds.generateOne
        _ <- upsertSubscriber(newSubscriberId, subscriberUrl, sourceUrl)

        _ <- delivery.registerSending(event, subscriberUrl).assertNoException
        _ <- findAllEventDeliveries.asserting(_ shouldBe List(FoundDelivery(event.compoundEventId, newSubscriberId)))
      } yield Succeeded
    }

  it should "update the delivery_id if the id is a DeletingProjectDeliverId" in testDBResource.use { implicit cfg =>
    for {
      _ <- addEvent(event.compoundEventId)
      _ <- upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)

      _ <- deliveryWhileDeletingProject.registerSending(event, subscriberUrl).assertNoException

      _ <- findAllProjectDeliveries.asserting {
             _ shouldBe
               List(FoundProjectDelivery(event.compoundEventId.projectId, subscriberId, DeletingProjectTypeId))
           }
    } yield Succeeded
  }

  it should "do nothing if the same event is delivered twice with a DeletingProjectTypeId" in testDBResource.use {
    implicit cfg =>
      for {
        _ <- addEvent(event.compoundEventId)
        _ <- upsertSubscriber(subscriberId, subscriberUrl, sourceUrl)

        _ <- deliveryWhileDeletingProject.registerSending(event, subscriberUrl).assertNoException

        _ <- findAllProjectDeliveries.asserting {
               _ shouldBe List(
                 FoundProjectDelivery(event.compoundEventId.projectId, subscriberId, DeletingProjectTypeId)
               )
             }

        _ <- delivery.registerSending(event, subscriberUrl).assertNoException

        _ <- findAllProjectDeliveries.asserting {
               _ shouldBe List(
                 FoundProjectDelivery(event.compoundEventId.projectId, subscriberId, DeletingProjectTypeId)
               )
             }
      } yield Succeeded
  }

  private implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]

  private def delivery(implicit cfg: DBConfig[EventLogDB]) = {
    val compoundIdExtractor: TestCompoundIdEvent => CompoundEventDeliveryId =
      e => CompoundEventDeliveryId(e.compoundEventId)
    new EventDeliveryImpl[IO, TestCompoundIdEvent](compoundIdExtractor, sourceUrl)
  }

  private def deliveryWhileDeletingProject(implicit cfg: DBConfig[EventLogDB]) = {
    val compoundIdExtractor: TestCompoundIdEvent => EventDeliveryId =
      event => DeletingProjectDeliverId(event.projectId)
    new EventDeliveryImpl[IO, TestCompoundIdEvent](compoundIdExtractor, sourceUrl)
  }

  private def addEvent(eventId: CompoundEventId)(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    storeEvent(eventId,
               eventStatuses.generateOne,
               executionDates.generateOne,
               eventDates.generateOne,
               eventBodies.generateOne
    )
}
