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

import cats.syntax.all._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators.{eventDates, executionDates}
import io.renku.eventlog.{EventMessage, ExecutionDate, InMemoryEventLogDbSpec}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.relativeTimestamps
import io.renku.graph.model.EventsGenerators.{compoundEventIds, eventBodies, eventStatuses, processingStatuses}
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.{CompoundEventId, EventStatus}
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class LostZombieEventFinderSpec extends AnyWordSpec with IOSpec with InMemoryEventLogDbSpec with should.Matchers {

  "popEvent" should {
    "do nothing if there are no zombie events in the table" in new TestCase {
      addRandomEvent()
      finder.popEvent().unsafeRunSync() shouldBe None
    }

    "do nothing if there are zombie events in the table but all were added less than 5 minutes ago" in new TestCase {
      addRandomEvent()
      addZombieEvent(compoundEventIds.generateOne, activeZombieEventExecutionDate.generateOne)
      addZombieEvent(compoundEventIds.generateOne, activeZombieEventExecutionDate.generateOne)

      finder.popEvent().unsafeRunSync() shouldBe None
    }

    "return a zombie event which has not been picked up" in new TestCase {
      addRandomEvent()
      addZombieEvent(compoundEventIds.generateOne, activeZombieEventExecutionDate.generateOne)

      val zombieEventId: CompoundEventId = compoundEventIds.generateOne
      val zombieEventStatus = processingStatuses.generateOne
      addZombieEvent(zombieEventId, lostZombieEventExecutionDate.generateOne, zombieEventStatus)

      finder.popEvent().unsafeRunSync() shouldBe ZombieEvent(finder.processName,
                                                             zombieEventId,
                                                             projectPath,
                                                             zombieEventStatus
      ).some

      finder.popEvent().unsafeRunSync() shouldBe None
    }

    "return None if an event is in the past and the status is GeneratingTriples or TransformingTriples " +
      "but the message is not a zombie message" in new TestCase {
        addRandomEvent(lostZombieEventExecutionDate.generateOne, processingStatuses.generateOne)
        addZombieEvent(compoundEventIds.generateOne, activeZombieEventExecutionDate.generateOne)

        finder.popEvent().unsafeRunSync() shouldBe None
      }
  }

  private trait TestCase {

    val executionDateThreshold = 5 * 60

    val activeZombieEventExecutionDate =
      relativeTimestamps(lessThanAgo = Duration.ofSeconds(executionDateThreshold - 2)).toGeneratorOf(ExecutionDate)

    val lostZombieEventExecutionDate =
      relativeTimestamps(moreThanAgo = Duration.ofSeconds(executionDateThreshold + 2)).toGeneratorOf(ExecutionDate)

    val projectPath = projectPaths.generateOne

    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val finder           = new LostZombieEventFinder(queriesExecTimes)

    def addRandomEvent(executionDate: ExecutionDate = executionDates.generateOne,
                       status:        EventStatus = eventStatuses.generateOne
    ): Unit = storeEvent(
      compoundEventIds.generateOne,
      status,
      executionDate,
      eventDates.generateOne,
      eventBodies.generateOne
    )

    def addZombieEvent(eventId:       CompoundEventId,
                       executionDate: ExecutionDate,
                       status:        EventStatus = processingStatuses.generateOne
    ): Unit = storeEvent(
      eventId,
      status,
      executionDate,
      eventDates.generateOne,
      eventBodies.generateOne,
      projectPath = projectPath,
      maybeMessage = Some(EventMessage(zombieMessage))
    )
  }
}
