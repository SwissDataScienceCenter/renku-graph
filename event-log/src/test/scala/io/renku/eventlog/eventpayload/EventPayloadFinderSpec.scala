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

package io.renku.eventlog.eventpayload

import cats.effect.IO
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.eventlog.eventpayload.EventPayloadFinder.PayloadData
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.events.{EventStatus, ZippedEventPayload}
import io.renku.graph.model.{EventContentGenerators, EventsGenerators, GraphModelGenerators}
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import scodec.bits.ByteVector

class EventPayloadFinderSpec extends AnyWordSpec with IOSpec with InMemoryEventLogDbSpec with should.Matchers {

  "findEventPayload" should {

    "return the payload if present" in {
      val eventId     = EventsGenerators.compoundEventIds.generateOne
      val projectSlug = GraphModelGenerators.projectSlugs.generateOne
      val payload     = ByteVector.fromValidHex("caffee")
      storeEvent(
        compoundEventId = eventId,
        eventStatus = EventStatus.TriplesStore,
        executionDate = EventContentGenerators.executionDates.generateOne,
        eventDate = EventContentGenerators.eventDates.generateOne,
        eventBody = EventsGenerators.eventBodies.generateOne,
        projectSlug = projectSlug,
        maybeEventPayload = Some(ZippedEventPayload(payload.toArray))
      )

      val finder = EventPayloadFinder[IO]
      val result = finder.findEventPayload(eventId.id, projectSlug).unsafeRunSync()
      result shouldBe Some(PayloadData(payload))
    }

    "return none if event is not present" in {
      val eventId     = EventsGenerators.compoundEventIds.generateOne
      val projectSlug = GraphModelGenerators.projectSlugs.generateOne
      val finder      = EventPayloadFinder[IO]
      val result      = finder.findEventPayload(eventId.id, projectSlug).unsafeRunSync()
      result shouldBe None
    }

    "return none if event payload is not present" in {
      val eventId     = EventsGenerators.compoundEventIds.generateOne
      val projectSlug = GraphModelGenerators.projectSlugs.generateOne
      storeEvent(
        compoundEventId = eventId,
        eventStatus = EventStatus.TriplesStore,
        executionDate = EventContentGenerators.executionDates.generateOne,
        eventDate = EventContentGenerators.eventDates.generateOne,
        eventBody = EventsGenerators.eventBodies.generateOne,
        projectSlug = projectSlug
      )

      val finder = EventPayloadFinder[IO]
      val result = finder.findEventPayload(eventId.id, projectSlug).unsafeRunSync()
      result shouldBe None
    }
  }

  private implicit lazy val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
  private implicit lazy val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
}
