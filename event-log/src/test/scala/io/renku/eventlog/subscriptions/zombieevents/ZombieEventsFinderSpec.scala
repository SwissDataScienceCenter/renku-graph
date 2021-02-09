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

import cats.syntax.all._
import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.{EventProcessingTime, ExecutionDate, InMemoryEventLogDbSpec}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.time.Instant.now

class ZombieEventsFinderSpec extends AnyWordSpec with InMemoryEventLogDbSpec with MockFactory with should.Matchers {

  "popEvent" should {

    "return an event if " +
      s"it's in the $GeneratingTriples status " +
      "there's no info about the last processing times for the project " +
      "and it's in the status for more than the MaxProcessingTime" in new TestCase {

        val eventId = compoundEventIds.generateOne
        addEvent(
          eventId,
          GeneratingTriples,
          relativeTimestamps(
            moreThanAgo = maxProcessingTime.value plusMinutes positiveInts().generateOne.value
          ).generateAs(ExecutionDate)
        )
        addEvent(
          compoundEventIds.generateOne,
          GeneratingTriples,
          relativeTimestamps(
            lessThanAgo = maxProcessingTime.value minusMinutes positiveInts(max = 59).generateOne.value
          ).generateAs(ExecutionDate)
        )

        finder.popEvent().unsafeRunSync() shouldBe Some(ZombieEvent(eventId, GeneratingTriples))
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return an event if " +
      s"it's in the $TransformingTriples status " +
      "there's no info about the last processing times for the project " +
      "and it's in the status for more than the MaxProcessingTime" in new TestCase {

        val eventId = compoundEventIds.generateOne
        addEvent(
          eventId,
          TransformingTriples,
          relativeTimestamps(
            moreThanAgo = maxProcessingTime.value plusMinutes positiveInts().generateOne.value
          ).generateAs(ExecutionDate)
        )
        addEvent(
          compoundEventIds.generateOne,
          TransformingTriples,
          relativeTimestamps(
            lessThanAgo = maxProcessingTime.value minusMinutes positiveInts(max = 59).generateOne.value
          ).generateAs(ExecutionDate)
        )

        finder.popEvent().unsafeRunSync() shouldBe Some(ZombieEvent(eventId, TransformingTriples))
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return an event if " +
      s"it's in the $GeneratingTriples status " +
      "there's info about the last processing times for the project " +
      "and it's in the status for more than the (median from last 3 events) * 2" in new TestCase {

        val projectId = projectIds.generateOne
        val oldProcessingTimes =
          eventProcessingTimes.generateNonEmptyList().toList.mapWithIndex { case (processingTime, idx) =>
            addEvent(idx,
                     projectId,
                     currentStatus = Gen.oneOf(TriplesGenerated, TriplesStore).generateOne,
                     processingInfo = GeneratingTriples -> processingTime
            )
            processingTime
          }

        val medianProcessingTime = {
          val threeMostRecentTimes = oldProcessingTimes.reverse.take(3)
          val sortedTimes          = threeMostRecentTimes.sorted.reverse
          sortedTimes(sortedTimes.size / 2)
        }

        val eventId = compoundEventIds.generateOne.copy(projectId = projectId)
        addEvent(
          eventId,
          GeneratingTriples,
          relativeTimestamps(
            moreThanAgo =
              (medianProcessingTime * maxProcessingTimeRatio).value plusMinutes positiveInts().generateOne.value
          ).generateAs(ExecutionDate)
        )
        addEvent(
          compoundEventIds.generateOne.copy(projectId = projectId),
          GeneratingTriples,
          relativeTimestamps(
            lessThanAgo = (medianProcessingTime * maxProcessingTimeRatio).value minusMinutes 2
          ).generateAs(ExecutionDate)
        )

        finder.popEvent().unsafeRunSync() shouldBe Some(ZombieEvent(eventId, GeneratingTriples))
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return an event if " +
      s"it's in the $TransformingTriples status " +
      "there's info about the last processing times for the project " +
      "and it's in the status for more than the (median from last 3 events) * 2" in new TestCase {

        val projectId = projectIds.generateOne
        val oldProcessingTimes =
          eventProcessingTimes.generateNonEmptyList().toList.mapWithIndex { case (processingTime, idx) =>
            addEvent(idx,
                     projectId,
                     currentStatus = TriplesStore,
                     processingInfo = TransformingTriples -> processingTime
            )
            processingTime
          }

        val medianProcessingTime = {
          val threeMostRecentTimes = oldProcessingTimes.reverse.take(3)
          val sortedTimes          = threeMostRecentTimes.sorted.reverse
          sortedTimes(sortedTimes.size / 2)
        }

        val eventId = compoundEventIds.generateOne.copy(projectId = projectId)
        addEvent(
          eventId,
          TransformingTriples,
          relativeTimestamps(
            moreThanAgo =
              (medianProcessingTime * maxProcessingTimeRatio).value plusMinutes positiveInts().generateOne.value
          ).generateAs(ExecutionDate)
        )
        addEvent(
          compoundEventIds.generateOne.copy(projectId = projectId),
          TransformingTriples,
          relativeTimestamps(
            lessThanAgo = (medianProcessingTime * maxProcessingTimeRatio).value minusMinutes 2
          ).generateAs(ExecutionDate)
        )

        finder.popEvent().unsafeRunSync() shouldBe Some(ZombieEvent(eventId, TransformingTriples))
        finder.popEvent().unsafeRunSync() shouldBe None
      }
  }

  private trait TestCase {

    val maxProcessingTime = javaDurations(min = Duration.ofHours(1)).generateAs(EventProcessingTime)
    val maxProcessingTimeRatio: Int Refined Positive = 2
    val queriesExecTimes = TestLabeledHistogram[SqlQuery.Name]("query_id")

    val finder = new ZombieEventsFinderImpl(transactor, maxProcessingTime, maxProcessingTimeRatio, queriesExecTimes)
  }

  private def addEvent(eventId: CompoundEventId, status: EventStatus, executionDate: ExecutionDate): Unit =
    storeEvent(
      eventId,
      status,
      executionDate,
      eventDates.generateOne,
      eventBodies.generateOne
    )

  private def addEvent(index:          Int,
                       projectId:      projects.Id,
                       currentStatus:  EventStatus,
                       processingInfo: (EventStatus, EventProcessingTime)
  ): Unit = {
    val eventId                  = compoundEventIds.generateOne.copy(projectId = projectId)
    val (status, processingTime) = processingInfo
    addEvent(eventId, currentStatus, ExecutionDate(now.minus(processingTime.value).plusSeconds(index + 5)))
    upsertProcessingTime(eventId, status, processingTime)
  }
}
