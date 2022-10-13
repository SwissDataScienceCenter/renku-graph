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

package io.renku.eventlog.eventdetails

import cats.syntax.all._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.{compoundEventIds, eventBodies, eventStatuses}
import io.renku.graph.model.events.{CompoundEventId, EventDetails}
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventDetailsFinderSpec extends AnyWordSpec with IOSpec with InMemoryEventLogDbSpec with should.Matchers {

  "findDetails" should {

    "return the details of the event if found" in new TestCase {
      storeEvent(
        eventId,
        eventStatuses.generateOne,
        executionDates.generateOne,
        eventDates.generateOne,
        eventBody
      )
      eventDetailsFinder.findDetails(eventId).unsafeRunSync() shouldBe EventDetails(eventId, eventBody).some
    }

    "return None if the event is not found" in new TestCase {
      eventDetailsFinder.findDetails(eventId).unsafeRunSync() shouldBe Option.empty[CompoundEventId]
    }
  }

  private trait TestCase {
    val eventId   = compoundEventIds.generateOne
    val eventBody = eventBodies.generateOne

    val queriesExecTimes   = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val eventDetailsFinder = new EventDetailsFinderImpl(queriesExecTimes)
  }
}
