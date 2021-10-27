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

package io.renku.eventlog.subscriptions.zombieevents

import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.{ExecutionDate, InMemoryEventLogDbSpec}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{CompoundEventId, EventStatus}
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class LongProcessingEventFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with should.Matchers {

  "popEvent" should {

    GeneratingTriples +: TransformingTriples +: Nil foreach { status =>
      "return no event if " +
        s"it's in the $status status " +
        "but there's delivery info for it" in new TestCase {

          val eventId = compoundEventIds.generateOne
          addEvent(
            eventId,
            GeneratingTriples,
            relativeTimestamps().generateAs(ExecutionDate)
          )
          upsertEventDeliveryInfo(eventId)

          finder.popEvent().unsafeRunSync() shouldBe None
        }
    }

    GeneratingTriples +: TransformingTriples +: Nil foreach { status =>
      "return an event if " +
        s"it's in the $status status " +
        "there's no info about its delivery " +
        "and it in status for more than 5 minutes" in new TestCase {

          val eventId = compoundEventIds.generateOne
          addEvent(
            eventId,
            status,
            relativeTimestamps(moreThanAgo = Duration ofMinutes 6).generateAs(ExecutionDate)
          )

          val eventInProcessingStatusTooShort = compoundEventIds.generateOne
          addEvent(
            eventInProcessingStatusTooShort,
            status,
            relativeTimestamps(lessThanAgo = Duration ofMinutes 4).generateAs(ExecutionDate)
          )

          finder.popEvent().unsafeRunSync() shouldBe Some(
            ZombieEvent(finder.processName, eventId, projectPath, status)
          )
          finder.popEvent().unsafeRunSync() shouldBe None
        }
    }
  }

  private trait TestCase {

    val projectPath = projectPaths.generateOne

    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")

    val finder = new LongProcessingEventFinder(sessionResource, queriesExecTimes)

    def addEvent(eventId: CompoundEventId, status: EventStatus, executionDate: ExecutionDate): Unit =
      storeEvent(
        eventId,
        status,
        executionDate,
        eventDates.generateOne,
        eventBodies.generateOne,
        projectPath = projectPath
      )
  }
}
