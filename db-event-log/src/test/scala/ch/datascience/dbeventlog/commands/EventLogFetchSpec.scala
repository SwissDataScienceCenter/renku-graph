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

import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog._
import EventStatus._
import cats.data.NonEmptyList
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events.{CommitEventId, ProjectId}
import doobie.implicits._
import eu.timepit.refined.auto._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogFetchSpec extends WordSpec with InMemoryEventLogDbSpec with MockFactory {

  import EventLogFetch._

  "isEventToProcess" should {

    s"return true if there are events with with status $New and execution date in the past" in new TestCase {
      storeNewEvent(commitEventIds.generateOne, ExecutionDate(now minus (5, SECONDS)), eventBodies.generateOne)

      eventLogFetch.isEventToProcess.unsafeRunSync() shouldBe true
    }

    s"return true if there are events with with status $RecoverableFailure and execution date in the past" in new TestCase {
      storeEvent(commitEventIds.generateOne,
                 EventStatus.RecoverableFailure,
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
                 EventStatus.RecoverableFailure,
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
      s"and status $New or $RecoverableFailure " +
      s"and mark it as $Processing" in new TestCase {

      val projectId = projectIds.generateOne

      val event1Id   = commitEventIds.generateOne.copy(projectId = projectId)
      val event1Body = eventBodies.generateOne
      storeNewEvent(event1Id, ExecutionDate(now minus (5, SECONDS)), event1Body)

      val event2Id   = commitEventIds.generateOne.copy(projectId = projectId)
      val event2Body = eventBodies.generateOne
      storeNewEvent(event2Id, ExecutionDate(now plus (5, HOURS)), event2Body)

      val event3Id   = commitEventIds.generateOne.copy(projectId = projectId)
      val event3Body = eventBodies.generateOne
      storeEvent(event3Id,
                 EventStatus.RecoverableFailure,
                 ExecutionDate(now minus (5, HOURS)),
                 committedDates.generateOne,
                 event3Body)

      findEvents(EventStatus.Processing) shouldBe List.empty

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe Some(event3Body)

      findEvents(EventStatus.Processing) shouldBe List(event3Id -> executionDate)

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe Some(event1Body)

      findEvents(EventStatus.Processing) shouldBe List(event1Id -> executionDate, event3Id -> executionDate)

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

      storeNewEvent(
        commitEventIds.generateOne,
        ExecutionDate(now plus (5, HOURS)),
        eventBodies.generateOne
      )

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe None
    }

    "find events not always from the same project " +
      "even if some projects events' farthest execution dates are later" in new TestCase {

      val allProjectIds = nonEmptyList(projectIds, minElements = 2).generateOne
      val eventIdsBodiesDates = for {
        projectId     <- allProjectIds
        eventId       <- nonEmptyList(commitIds, minElements = 5).generateOne map (CommitEventId(_, projectId))
        eventBody     <- nonEmptyList(eventBodies, maxElements = 1).generateOne
        executionDate <- NonEmptyList.of(executionDateDifferentiated(by = projectId, allProjectIds))
      } yield (eventId, executionDate, eventBody)

      eventIdsBodiesDates.toList foreach {
        case (eventId, eventExecutionDate, eventBody) => storeNewEvent(eventId, eventExecutionDate, eventBody)
      }

      findEvents(EventStatus.Processing) shouldBe List.empty

      val eventsFetcher = new EventLogFetch(transactor)
      eventIdsBodiesDates.toList foreach { _ =>
        eventsFetcher.popEventToProcess.unsafeRunSync() shouldBe a[Some[_]]
      }

      val commitEventsByExecutionOrder = findEvents(
        status  = Processing,
        orderBy = fr"execution_date asc"
      ).map(_._1)
      val commitEventsByExecutionDate = eventIdsBodiesDates.map(_._1)
      commitEventsByExecutionOrder.map(_.projectId) should not be commitEventsByExecutionDate.map(_.projectId).toList
    }
  }

  private trait TestCase {

    val currentTime   = mockFunction[Instant]
    val eventLogFetch = new EventLogFetch(transactor, currentTime)

    val now           = Instant.now()
    val executionDate = ExecutionDate(now)
    currentTime.expects().returning(now).anyNumberOfTimes()

    def executionDateDifferentiated(by: ProjectId, allProjects: NonEmptyList[ProjectId]) =
      ExecutionDate(now minus (1000 - (allProjects.toList.indexOf(by) * 10), SECONDS))
  }

  private def storeNewEvent(commitEventId: CommitEventId, executionDate: ExecutionDate, eventBody: EventBody): Unit =
    storeEvent(commitEventId, New, executionDate, committedDates.generateOne, eventBody)
}
