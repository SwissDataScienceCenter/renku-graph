/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.dbeventlog.commands

import java.time.Instant
import java.time.temporal.ChronoUnit._

import cats.implicits._
import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog._
import EventStatus._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.EventsGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogFetchSpec extends WordSpec with InMemoryEventLogDbSpec with MockFactory {

  import EventLogFetch._

  "isEventToProcess" should {

    s"return true if there are events with with status $New and execution date in the past" in new TestCase {
      storeEvent(commitEventIds.generateOne,
                 EventStatus.New,
                 ExecutionDate(now minus (5, SECONDS)),
                 committedDates.generateOne,
                 eventBodies.generateOne)

      eventLogFetch.isEventToProcess.unsafeRunSync() shouldBe true
    }

    s"return true if there are events with with status $TriplesStoreFailure and execution date in the past" in new TestCase {
      storeEvent(commitEventIds.generateOne,
                 EventStatus.TriplesStoreFailure,
                 ExecutionDate(now minus (5, SECONDS)),
                 committedDates.generateOne,
                 eventBodies.generateOne)

      eventLogFetch.isEventToProcess.unsafeRunSync() shouldBe true
    }

    s"return true if there are events with with status $Processing and execution date older than $MaxProcessingTime" in new TestCase {
      storeEvent(
        commitEventIds.generateOne,
        EventStatus.Processing,
        ExecutionDate(now minus (MaxProcessingTime.toMinutes + 1, MINUTES)),
        committedDates.generateOne,
        eventBodies.generateOne
      )

      eventLogFetch.isEventToProcess.unsafeRunSync() shouldBe true
    }

    s"return false for other cases" in new TestCase {
      storeEvent(
        commitEventIds.generateOne,
        EventStatus.Processing,
        ExecutionDate(now minus (MaxProcessingTime.toMinutes - 1, MINUTES)),
        committedDates.generateOne,
        eventBodies.generateOne
      )
      storeEvent(commitEventIds.generateOne,
                 EventStatus.New,
                 ExecutionDate(now plus (5, SECONDS)),
                 committedDates.generateOne,
                 eventBodies.generateOne)
      storeEvent(commitEventIds.generateOne,
                 EventStatus.TriplesStoreFailure,
                 ExecutionDate(now plus (5, SECONDS)),
                 committedDates.generateOne,
                 eventBodies.generateOne)
      storeEvent(commitEventIds.generateOne,
                 EventStatus.NonRecoverableFailure,
                 ExecutionDate(now minus (5, SECONDS)),
                 committedDates.generateOne,
                 eventBodies.generateOne)
      storeEvent(commitEventIds.generateOne,
                 EventStatus.TriplesStore,
                 ExecutionDate(now minus (5, SECONDS)),
                 committedDates.generateOne,
                 eventBodies.generateOne)

      eventLogFetch.isEventToProcess.unsafeRunSync() shouldBe false
    }
  }

  "popEventToProcess" should {

    "find event with execution date farthest in the past " +
      s"and status $New or $TriplesStoreFailure " +
      s"and mark it as $Processing" in new TestCase {

      val event1Id   = commitEventIds.generateOne
      val event1Body = eventBodies.generateOne
      storeEvent(event1Id,
                 EventStatus.New,
                 ExecutionDate(now minus (5, SECONDS)),
                 committedDates.generateOne,
                 event1Body)

      val event2Id   = commitEventIds.generateOne
      val event2Body = eventBodies.generateOne
      storeEvent(event2Id, EventStatus.New, ExecutionDate(now plus (5, HOURS)), committedDates.generateOne, event2Body)

      val event3Id   = commitEventIds.generateOne
      val event3Body = eventBodies.generateOne
      storeEvent(event3Id,
                 EventStatus.TriplesStoreFailure,
                 ExecutionDate(now minus (5, HOURS)),
                 committedDates.generateOne,
                 event3Body)

      findEvents(EventStatus.Processing) shouldBe List.empty

      parallelPopEventToProcess.unsafeRunSync() shouldBe List(event3Body)

      findEvents(EventStatus.Processing) shouldBe List(event3Id -> executionDate)

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe Some(event1Body)

      findEvents(EventStatus.Processing) shouldBe List(event1Id -> executionDate, event3Id -> executionDate)

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe None
    }

    "find event with execution date farthest in the past " +
      "event if there are more events with the same id" in new TestCase {

      val sameEventId = commitIds.generateOne
      val event1Id    = commitEventIds.generateOne.copy(id = sameEventId)
      val eventBody1  = eventBodies.generateOne
      storeEvent(event1Id,
                 EventStatus.New,
                 ExecutionDate(now minus (5, SECONDS)),
                 committedDates.generateOne,
                 eventBody1)

      val eventBody2 = eventBodies.generateOne
      val event2Id   = commitEventIds.generateOne.copy(id = sameEventId)
      storeEvent(event2Id, EventStatus.New, ExecutionDate(now minus (5, HOURS)), committedDates.generateOne, eventBody2)

      findEvents(EventStatus.Processing) shouldBe List.empty

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe Some(eventBody2)

      findEvents(EventStatus.Processing) shouldBe List(event2Id -> executionDate)

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe Some(eventBody1)

      findEvents(EventStatus.Processing) shouldBe List(event1Id -> executionDate, event2Id -> executionDate)

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe None
    }

    s"find event with the $Processing status " +
      s"and execution date older than $MaxProcessingTime" in new TestCase {

      val eventId   = commitEventIds.generateOne
      val eventBody = eventBodies.generateOne
      storeEvent(eventId,
                 EventStatus.Processing,
                 ExecutionDate(now minus (MaxProcessingTime.toMinutes + 1, MINUTES)),
                 committedDates.generateOne,
                 eventBody)

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe Some(eventBody)

      findEvents(EventStatus.Processing) shouldBe List(eventId -> executionDate)
    }

    s"find no event when there there's one with $Processing status " +
      "but execution date from less than from 10 mins ago" in new TestCase {

      storeEvent(
        commitEventIds.generateOne,
        EventStatus.Processing,
        ExecutionDate(now minus (9, MINUTES)),
        committedDates.generateOne,
        eventBodies.generateOne
      )

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe None
    }

    "find no events when there are no events matching the criteria" in new TestCase {

      storeEvent(
        commitEventIds.generateOne,
        EventStatus.New,
        ExecutionDate(now plus (5, HOURS)),
        committedDates.generateOne,
        eventBodies.generateOne
      )

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {

    val currentTime   = mockFunction[Instant]
    val eventLogFetch = new EventLogFetch(transactor, currentTime)

    val now           = Instant.now()
    val executionDate = ExecutionDate(now)
    currentTime.expects().returning(now).anyNumberOfTimes()

    def parallelPopEventToProcess =
      List(
        eventLogFetch.popEventToProcess,
        eventLogFetch.popEventToProcess
      ).parSequence.map(_.flatten)
  }
}
