/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import java.time.temporal.ChronoUnit.{HOURS => H, MINUTES => MIN, SECONDS => SEC}
import java.time.{Duration, Instant}

import cats.implicits._
import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog._
import EventStatus._
import cats.data.NonEmptyList
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.{BatchDate, CommitEventId}
import ch.datascience.graph.model.projects.Id
import doobie.implicits._
import eu.timepit.refined.auto._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.language.postfixOps

class EventLogFetchSpec extends WordSpec with InMemoryEventLogDbSpec with MockFactory {

  "isEventToProcess" should {

    s"return true if there are events with with status $New and execution date in the past" in new TestCase {
      storeNewEvent(commitEventIds.generateOne, ExecutionDate(now minus (5, SEC)), eventBodies.generateOne)

      eventLogFetch.isEventToProcess.unsafeRunSync() shouldBe true
    }

    s"return true if there are events with with status $RecoverableFailure and execution date in the past" in new TestCase {
      storeEvent(commitEventIds.generateOne,
                 EventStatus.RecoverableFailure,
                 ExecutionDate(now minus (5, SEC)),
                 committedDates.generateOne,
                 eventBodies.generateOne)

      eventLogFetch.isEventToProcess.unsafeRunSync() shouldBe true
    }

    s"return true if there are events with with status $Processing " +
      "and execution date older than the given RenkuLogTimeout + 5 min" in new TestCase {
      storeEvent(
        commitEventIds.generateOne,
        EventStatus.Processing,
        ExecutionDate(now minus (maxProcessingTime.toMinutes + 5, MIN)),
        committedDates.generateOne,
        eventBodies.generateOne
      )

      eventLogFetch.isEventToProcess.unsafeRunSync() shouldBe true
    }

    s"return false for other cases" in new TestCase {
      storeEvent(
        commitEventIds.generateOne,
        EventStatus.Processing,
        ExecutionDate(now minus (maxProcessingTime.toMinutes - 1, MIN)),
        committedDates.generateOne,
        eventBodies.generateOne
      )
      storeEvent(commitEventIds.generateOne,
                 EventStatus.New,
                 ExecutionDate(now plus (5, SEC)),
                 committedDates.generateOne,
                 eventBodies.generateOne)
      storeEvent(commitEventIds.generateOne,
                 EventStatus.RecoverableFailure,
                 ExecutionDate(now plus (5, SEC)),
                 committedDates.generateOne,
                 eventBodies.generateOne)
      storeEvent(commitEventIds.generateOne,
                 EventStatus.NonRecoverableFailure,
                 ExecutionDate(now minus (5, SEC)),
                 committedDates.generateOne,
                 eventBodies.generateOne)
      storeEvent(commitEventIds.generateOne,
                 EventStatus.TriplesStore,
                 ExecutionDate(now minus (5, SEC)),
                 committedDates.generateOne,
                 eventBodies.generateOne)

      eventLogFetch.isEventToProcess.unsafeRunSync() shouldBe false
    }
  }

  "popEventToProcess" should {

    "return an event with execution date farthest in the past " +
      s"and status $New or $RecoverableFailure " +
      s"and mark it as $Processing" in new TestCase {

      val projectId = projectIds.generateOne

      val event1Id        = commitEventIds.generateOne.copy(projectId = projectId)
      val event1Body      = eventBodies.generateOne
      val event1BatchDate = batchDates.generateOne
      storeNewEvent(event1Id, ExecutionDate(now minus (5, SEC)), event1Body, batchDate = event1BatchDate)

      val event2Id   = commitEventIds.generateOne.copy(projectId = projectId)
      val event2Body = eventBodies.generateOne
      storeNewEvent(event2Id, ExecutionDate(now plus (5, H)), event2Body)

      val event3Id        = commitEventIds.generateOne.copy(projectId = projectId)
      val event3Body      = eventBodies.generateOne
      val event3BatchDate = batchDates.generateOne
      storeEvent(event3Id,
                 EventStatus.RecoverableFailure,
                 ExecutionDate(now minus (5, H)),
                 committedDates.generateOne,
                 event3Body,
                 batchDate = event3BatchDate)

      findEvents(EventStatus.Processing) shouldBe List.empty

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe Some(event3Body)

      findEvents(EventStatus.Processing) shouldBe List((event3Id, executionDate, event3BatchDate))

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe Some(event1Body)

      findEvents(EventStatus.Processing) shouldBe List((event1Id, executionDate, event1BatchDate),
                                                       (event3Id, executionDate, event3BatchDate))

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe None
    }

    s"return an event with the $Processing status " +
      "and execution date older than RenkuLogTimeout + 5 min" in new TestCase {

      val eventId        = commitEventIds.generateOne
      val eventBody      = eventBodies.generateOne
      val eventBatchDate = batchDates.generateOne
      storeEvent(
        eventId,
        EventStatus.Processing,
        ExecutionDate(now minus (maxProcessingTime.toMinutes + 5, MIN)),
        committedDates.generateOne,
        eventBody,
        batchDate = eventBatchDate
      )

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe Some(eventBody)

      findEvents(EventStatus.Processing) shouldBe List((eventId, executionDate, eventBatchDate))
    }

    s"return no event when there there's one with $Processing status " +
      "but execution date from less than RenkuLogTimeout + 1 min" in new TestCase {

      storeEvent(
        commitEventIds.generateOne,
        EventStatus.Processing,
        ExecutionDate(now minus (maxProcessingTime minusSeconds 1)),
        committedDates.generateOne,
        eventBodies.generateOne
      )

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe None
    }

    "return no events when there are no events matching the criteria" in new TestCase {

      storeNewEvent(
        commitEventIds.generateOne,
        ExecutionDate(now plus (50, H)),
        eventBodies.generateOne
      )

      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe None
    }

    "return events from various projects " +
      "even if some projects have events with execution dates far in the past" in new TestCase {

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

      override val eventLogFetch = new EventLogFetchImpl(transactor, renkuLogTimeout)
      eventIdsBodiesDates.toList foreach { _ =>
        eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe a[Some[_]]
      }

      val commitEventsByExecutionOrder = findEvents(
        status  = Processing,
        orderBy = fr"execution_date asc"
      ).map(_._1)
      val commitEventsByExecutionDate = eventIdsBodiesDates.map(_._1)
      commitEventsByExecutionOrder.map(_.projectId) should not be commitEventsByExecutionDate.map(_.projectId).toList
    }

    "return events from various projects " +
      "even if there are events with execution dates far in the past " +
      s"and $Processing status " in new TestCase {

      val projectId1 = projectIds.generateOne
      storeEvent(
        CommitEventId(commitIds.generateOne, projectId1),
        EventStatus.Processing,
        ExecutionDate(now minus (maxProcessingTime.toMinutes + 1, MIN)),
        committedDates.generateOne,
        eventBodies.generateOne
      )
      val projectId2 = projectIds.generateOne
      storeEvent(
        CommitEventId(commitIds.generateOne, projectId2),
        EventStatus.New,
        ExecutionDate(now minus (1, MIN)),
        committedDates.generateOne,
        eventBodies.generateOne
      )

      findEvents(EventStatus.Processing).map(_._1.projectId) shouldBe List(projectId1)

      override val eventLogFetch = new EventLogFetchImpl(
        transactor,
        renkuLogTimeout,
        pickRandomlyFrom = _ => projectId2.some
      )
      eventLogFetch.popEventToProcess.unsafeRunSync() shouldBe a[Some[_]]

      findEvents(
        status  = Processing,
        orderBy = fr"execution_date asc"
      ).map(_._1.projectId) should contain theSameElementsAs List(projectId1, projectId2)
    }
  }

  private trait TestCase {

    val currentTime       = mockFunction[Instant]
    val renkuLogTimeout   = renkuLogTimeouts.generateOne
    val maxProcessingTime = renkuLogTimeout.toUnsafe[Duration] plusMinutes 5
    val eventLogFetch     = new EventLogFetchImpl(transactor, renkuLogTimeout, currentTime)

    val now           = Instant.now()
    val executionDate = ExecutionDate(now)
    currentTime.expects().returning(now).anyNumberOfTimes()

    def executionDateDifferentiated(by: Id, allProjects: NonEmptyList[Id]) =
      ExecutionDate(now minus (1000 - (allProjects.toList.indexOf(by) * 10), SEC))
  }

  private def storeNewEvent(commitEventId: CommitEventId,
                            executionDate: ExecutionDate,
                            eventBody:     EventBody,
                            batchDate:     BatchDate = batchDates.generateOne): Unit =
    storeEvent(commitEventId, New, executionDate, committedDates.generateOne, eventBody, batchDate = batchDate)
}
