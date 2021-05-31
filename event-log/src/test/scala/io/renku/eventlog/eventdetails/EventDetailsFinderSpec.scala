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

package io.renku.eventlog.eventdetails

import cats.syntax.all._
import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.{commitIds, compoundEventIds, eventBodies, eventStatuses}
import ch.datascience.graph.model.events.{CommitId, CompoundEventId, EventBody, EventDetails}
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.circe.literal.JsonStringContext
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.InMemoryEventLogDbSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventDetailsFinderSpec extends AnyWordSpec with InMemoryEventLogDbSpec with should.Matchers {

  "findDetails" should {

    "return the details of the event if found case where there are no parents" in new TestCase {
      storeEvent(
        eventId,
        eventStatuses.generateOne,
        executionDates.generateOne,
        eventDates.generateOne,
        eventBodies.generateOne
      )
      eventDetailsFinder.findDetails(eventId).unsafeRunSync() shouldBe EventDetails(eventId, List.empty[CommitId]).some
    }

    "return the details of the event if found" in new TestCase {
      val parents = commitIds.generateNonEmptyList().toList
      val eventBodyWithParents = jsons
        .map(json => json.deepMerge(json"""{"parents": ${parents.map(_.value)}}""").noSpaces)
        .map(EventBody.apply)

      storeEvent(
        eventId,
        eventStatuses.generateOne,
        executionDates.generateOne,
        eventDates.generateOne,
        eventBodyWithParents.generateOne
      )
      eventDetailsFinder.findDetails(eventId).unsafeRunSync() shouldBe EventDetails(eventId, parents).some
    }

    "return None if the event is not found" in new TestCase {
      eventDetailsFinder.findDetails(eventId).unsafeRunSync() shouldBe Option.empty[CompoundEventId]
    }
  }

  private trait TestCase {
    val eventId            = compoundEventIds.generateOne
    val queriesExecTimes   = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val eventDetailsFinder = new EventDetailsFinderImpl(sessionResource, queriesExecTimes)
  }
}
