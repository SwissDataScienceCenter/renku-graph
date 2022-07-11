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

package io.renku.eventlog.events.producers.zombieevents

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
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class LongProcessingEventFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with TableDrivenPropertyChecks
    with MockFactory
    with should.Matchers {

  "popEvent" should {

    forAll {
      Table(
        "status"            -> "grace period",
        GeneratingTriples   -> Duration.ofDays(6 * 7),
        TransformingTriples -> Duration.ofDays(1),
        Deleting            -> Duration.ofDays(1)
      )
    } { (status, gracePeriod) =>
      "return an event " +
        s"if it's in the $status status for more than $gracePeriod " +
        "and there's info about its delivery" in new TestCase {

          val oldEnoughEventId = compoundEventIds.generateOne
          addEvent(
            oldEnoughEventId,
            status,
            relativeTimestamps(moreThanAgo = gracePeriod plusHours 1).generateAs(ExecutionDate)
          )
          upsertEventDeliveryInfo(oldEnoughEventId)

          val tooYoungEventId = compoundEventIds.generateOne
          addEvent(
            tooYoungEventId,
            status,
            relativeTimestamps(lessThanAgo = gracePeriod minusHours 1).generateAs(ExecutionDate)
          )
          upsertEventDeliveryInfo(tooYoungEventId)

          finder.popEvent().unsafeRunSync() shouldBe Some(
            ZombieEvent(finder.processName, oldEnoughEventId, projectPath, status)
          )
          finder.popEvent().unsafeRunSync() shouldBe None
        }
    }

    GeneratingTriples :: TransformingTriples :: Deleting :: Nil foreach { status =>
      "return an event " +
        s"if it's in the $status status " +
        "there's no info about its delivery " +
        "and it's in status for more than 5 minutes" in new TestCase {

          val eventId = compoundEventIds.generateOne
          addEvent(
            eventId,
            status,
            relativeTimestamps(moreThanAgo = Duration ofMinutes 6).generateAs(ExecutionDate)
          )

          addEvent(
            compoundEventIds.generateOne,
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
    val finder           = new LongProcessingEventFinder(queriesExecTimes)

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
