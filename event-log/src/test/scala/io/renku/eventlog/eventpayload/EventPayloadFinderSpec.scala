/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.eventpayload

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.eventlog.EventLogPostgresSpec
import io.renku.eventlog.eventpayload.EventPayloadFinder.PayloadData
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.compoundEventIds
import io.renku.graph.model.GraphModelGenerators.projectSlugs
import io.renku.graph.model.events.{EventStatus, ZippedEventPayload}
import io.renku.graph.model.{EventContentGenerators, EventsGenerators, GraphModelGenerators}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import scodec.bits.ByteVector

class EventPayloadFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {

  it should "return the payload if present" in testDBResource.use { implicit cfg =>
    val eventId     = compoundEventIds.generateOne
    val projectSlug = GraphModelGenerators.projectSlugs.generateOne
    val payload     = ByteVector.fromValidHex("caffee")
    for {
      _ <- storeEvent(
             compoundEventId = eventId,
             eventStatus = EventStatus.TriplesStore,
             executionDate = EventContentGenerators.executionDates.generateOne,
             eventDate = EventContentGenerators.eventDates.generateOne,
             eventBody = EventsGenerators.eventBodies.generateOne,
             projectSlug = projectSlug,
             maybeEventPayload = Some(ZippedEventPayload(payload.toArray))
           )
      _ <- EventPayloadFinder[IO].findEventPayload(eventId.id, projectSlug).asserting {
             _ shouldBe Some(PayloadData(payload))
           }
    } yield Succeeded
  }

  it should "return none if event is not present" in testDBResource.use { implicit cfg =>
    EventPayloadFinder[IO]
      .findEventPayload(compoundEventIds.generateOne.id, projectSlugs.generateOne)
      .asserting(_ shouldBe None)
  }

  it should "return none if event payload is not present" in testDBResource.use { implicit cfg =>
    val eventId     = compoundEventIds.generateOne
    val projectSlug = GraphModelGenerators.projectSlugs.generateOne
    for {
      _ <- storeEvent(
             compoundEventId = eventId,
             eventStatus = EventStatus.TriplesStore,
             executionDate = EventContentGenerators.executionDates.generateOne,
             eventDate = EventContentGenerators.eventDates.generateOne,
             eventBody = EventsGenerators.eventBodies.generateOne,
             projectSlug = projectSlug
           )

      _ <- EventPayloadFinder[IO]
             .findEventPayload(eventId.id, projectSlug)
             .asserting(_ shouldBe None)
    } yield Succeeded
  }

  private implicit lazy val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
}
