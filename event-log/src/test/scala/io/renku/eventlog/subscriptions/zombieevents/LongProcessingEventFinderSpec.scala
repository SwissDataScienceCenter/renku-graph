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

import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.{ExecutionDate, InMemoryEventLogDbSpec}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class LongProcessingEventFinderSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with should.Matchers {

  "popEvent" should {

    "return an event if " +
      s"it's in the $GeneratingTriples status " +
      "there's no info about the last processing times for the project, " +
      "there's delivery info for it " +
      "and it's in the status for more than the MaxProcessingTime" in new TestCase {

        val eventId = compoundEventIds.generateOne
        addEvent(
          eventId,
          GeneratingTriples,
          relativeTimestamps(
            moreThanAgo = maxProcessingTime.value plusMinutes positiveInts().generateOne.value
          ).generateAs(ExecutionDate)
        )
        upsertEventDeliveryInfo(eventId)

        val eventInProcessingStatusTooShort = compoundEventIds.generateOne
        addEvent(
          eventInProcessingStatusTooShort,
          GeneratingTriples,
          relativeTimestamps(
            lessThanAgo = maxProcessingTime.value minusMinutes positiveInts(max = 59).generateOne.value
          ).generateAs(ExecutionDate)
        )
        upsertEventDeliveryInfo(eventInProcessingStatusTooShort)

        finder.popEvent().unsafeRunSync() shouldBe Some(
          ZombieEvent(finder.processName, eventId, projectPath, GeneratingTriples)
        )
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return an event if " +
      s"it's in the $TransformingTriples status " +
      "there's no info about the last processing times for the project " +
      "there's delivery info for it " +
      "and it's in the status for more than the MaxProcessingTime" in new TestCase {

        val eventId = compoundEventIds.generateOne
        addEvent(
          eventId,
          TransformingTriples,
          relativeTimestamps(
            moreThanAgo = maxProcessingTime.value plusMinutes positiveInts().generateOne.value
          ).generateAs(ExecutionDate)
        )
        upsertEventDeliveryInfo(eventId)

        val eventInProcessingStatusTooShort = compoundEventIds.generateOne
        addEvent(
          eventInProcessingStatusTooShort,
          TransformingTriples,
          relativeTimestamps(
            lessThanAgo = maxProcessingTime.value minusMinutes positiveInts(max = 59).generateOne.value
          ).generateAs(ExecutionDate)
        )
        upsertEventDeliveryInfo(eventInProcessingStatusTooShort)

        finder.popEvent().unsafeRunSync() shouldBe Some(
          ZombieEvent(finder.processName, eventId, projectPath, TransformingTriples)
        )
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return an event if " +
      s"it's in the $GeneratingTriples status " +
      "there's info about the last processing times for the project " +
      "there's delivery info for it " +
      "and it's in the status for more than the (median from last 3 events) * 2 " +
      "case when median is more than 2.5 min" in new TestCase {

        val projectId = projectIds.generateOne
        val datesAndProcessingTimes = {
          for {
            executionDate  <- executionDates
            processingTime <- longEventProcessingTimes
          } yield addEvent(projectId,
                           executionDate,
                           currentEventStatus = Gen.oneOf(TriplesGenerated, TriplesStore).generateOne,
                           processingTime,
                           processingTimeStatus = TriplesGenerated
          )
        }.generateNonEmptyList().toList

        val medianProcessingTime = {
          val threeMostEvents = datesAndProcessingTimes.sortBy(_._1).reverse.take(3)
          val sortedTimes     = threeMostEvents.map(_._2).sorted.reverse
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
        upsertEventDeliveryInfo(eventId)

        val eventInProcessingStatusTooShort = compoundEventIds.generateOne.copy(projectId = projectId)
        addEvent(
          eventInProcessingStatusTooShort,
          GeneratingTriples,
          relativeTimestamps(
            lessThanAgo = (medianProcessingTime * maxProcessingTimeRatio).value minusMinutes 2
          ).generateAs(ExecutionDate)
        )
        upsertEventDeliveryInfo(eventInProcessingStatusTooShort)

        finder.popEvent().unsafeRunSync() shouldBe Some(
          ZombieEvent(finder.processName, eventId, projectPath, GeneratingTriples)
        )
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return an event if " +
      s"it's in the $GeneratingTriples status " +
      "there's info about the last processing times for the project " +
      "there's delivery info for it " +
      "and it's in the status for more than the (median from last 3 events) * 2 " +
      "case when median is less than 2.5 min" in new TestCase {

        val projectId = projectIds.generateOne

        {
          for {
            executionDate  <- executionDates
            processingTime <- shortEventProcessingTimes
          } yield addEvent(projectId,
                           executionDate,
                           currentEventStatus = TriplesStore,
                           processingTime,
                           processingTimeStatus = TriplesStore
          )
        }.generateNonEmptyList().toList

        val eventId = compoundEventIds.generateOne.copy(projectId = projectId)
        addEvent(
          eventId,
          GeneratingTriples,
          relativeTimestamps(
            moreThanAgo = (Duration ofSeconds 150) plusMinutes positiveInts().generateOne.value
          ).generateAs(ExecutionDate)
        )
        upsertEventDeliveryInfo(eventId)

        val eventInProcessingStatusTooShort = compoundEventIds.generateOne.copy(projectId = projectId)
        addEvent(
          eventInProcessingStatusTooShort,
          GeneratingTriples,
          relativeTimestamps(
            lessThanAgo = Duration ofSeconds 145
          ).generateAs(ExecutionDate)
        )
        upsertEventDeliveryInfo(eventInProcessingStatusTooShort)

        finder.popEvent().unsafeRunSync() shouldBe Some(
          ZombieEvent(finder.processName, eventId, projectPath, GeneratingTriples)
        )
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return an event if " +
      s"it's in the $TransformingTriples status " +
      "there's info about the last processing times for the project " +
      "there's delivery info for it " +
      "and it's in the status for more than the (median from last 3 events) * 2 " +
      "case when median is more than 2.5 min" in new TestCase {

        val projectId = projectIds.generateOne
        val datesAndProcessingTimes = {
          for {
            executionDate  <- executionDates
            processingTime <- longEventProcessingTimes
          } yield addEvent(projectId,
                           executionDate,
                           currentEventStatus = TriplesStore,
                           processingTime,
                           processingTimeStatus = TriplesStore
          )
        }.generateNonEmptyList().toList

        val medianProcessingTime = {
          val threeMostEvents = datesAndProcessingTimes.sortBy(_._1).reverse.take(3)
          val sortedTimes     = threeMostEvents.map(_._2).sorted.reverse
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
        upsertEventDeliveryInfo(eventId)

        val eventInProcessingStatusTooShort = compoundEventIds.generateOne.copy(projectId = projectId)
        addEvent(
          eventInProcessingStatusTooShort,
          TransformingTriples,
          relativeTimestamps(
            lessThanAgo = (medianProcessingTime * maxProcessingTimeRatio).value minusMinutes 2
          ).generateAs(ExecutionDate)
        )
        upsertEventDeliveryInfo(eventInProcessingStatusTooShort)

        finder.popEvent().unsafeRunSync() shouldBe Some(
          ZombieEvent(finder.processName, eventId, projectPath, TransformingTriples)
        )
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return an event if " +
      s"it's in the $TransformingTriples status " +
      "there's info about the last processing times for the project " +
      "there's delivery info for it " +
      "and it's in the status for more than the (median from last 3 events) * 2 " +
      "case when median is less than 2.5 min" in new TestCase {

        val projectId = projectIds.generateOne

        {
          for {
            executionDate  <- executionDates
            processingTime <- shortEventProcessingTimes
          } yield addEvent(projectId,
                           executionDate,
                           currentEventStatus = TriplesStore,
                           processingTime,
                           processingTimeStatus = TriplesStore
          )
        }.generateNonEmptyList().toList

        val eventId = compoundEventIds.generateOne.copy(projectId = projectId)
        addEvent(
          eventId,
          TransformingTriples,
          relativeTimestamps(
            moreThanAgo = (Duration ofSeconds 150) plusMinutes positiveInts().generateOne.value
          ).generateAs(ExecutionDate)
        )
        upsertEventDeliveryInfo(eventId)

        val eventInProcessingStatusTooShort = compoundEventIds.generateOne.copy(projectId = projectId)
        addEvent(
          eventInProcessingStatusTooShort,
          TransformingTriples,
          relativeTimestamps(
            lessThanAgo = Duration ofSeconds 145
          ).generateAs(ExecutionDate)
        )
        upsertEventDeliveryInfo(eventInProcessingStatusTooShort)

        finder.popEvent().unsafeRunSync() shouldBe Some(
          ZombieEvent(finder.processName, eventId, projectPath, TransformingTriples)
        )
        finder.popEvent().unsafeRunSync() shouldBe None
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

    val maxProcessingTime = javaDurations(min = Duration.ofHours(1)).generateAs(EventProcessingTime)
    val maxProcessingTimeRatio: Int Refined Positive = 2
    val queriesExecTimes = TestLabeledHistogram[SqlQuery.Name]("query_id")

    val finder =
      new LongProcessingEventFinder(transactor, maxProcessingTime, maxProcessingTimeRatio, queriesExecTimes)

    def addEvent(eventId: CompoundEventId, status: EventStatus, executionDate: ExecutionDate): Unit = storeEvent(
      eventId,
      status,
      executionDate,
      eventDates.generateOne,
      eventBodies.generateOne,
      projectPath = projectPath
    )

    def addEvent(projectId:            projects.Id,
                 executionDate:        ExecutionDate,
                 currentEventStatus:   EventStatus,
                 processingTime:       EventProcessingTime,
                 processingTimeStatus: EventStatus
    ): (ExecutionDate, EventProcessingTime) = {
      val eventId = compoundEventIds.generateOne.copy(projectId = projectId)
      addEvent(eventId, currentEventStatus, executionDate)
      upsertProcessingTime(eventId, processingTimeStatus, processingTime)
      executionDate -> processingTime
    }
  }

  private lazy val longEventProcessingTimes: Gen[EventProcessingTime] =
    javaDurations(min = Duration ofSeconds 155).map(EventProcessingTime.apply)

  private lazy val shortEventProcessingTimes: Gen[EventProcessingTime] =
    javaDurations(max = Duration ofSeconds 145).map(EventProcessingTime.apply)
}
