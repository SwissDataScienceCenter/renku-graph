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

package io.renku.eventlog.subscriptions

import java.time.temporal.ChronoUnit.{HOURS => H, MINUTES => MIN, SECONDS => SEC}
import java.time.{Duration, Instant}

import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators.renkuLogTimeouts
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.{BatchDate, CompoundEventId, EventBody}
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.metrics.LabeledGauge
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.DbEventLogGenerators._
import io.renku.eventlog.EventStatus._
import io.renku.eventlog._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.language.postfixOps

class EventFetcherSpec extends WordSpec with InMemoryEventLogDbSpec with MockFactory {

  "popEvent" should {

    "return an event with execution date farthest in the past " +
      s"and status $New or $RecoverableFailure " +
      s"and mark it as $Processing" in new TestCase {

      val projectId   = projectIds.generateOne
      val projectPath = projectPaths.generateOne

      val event1Id        = compoundEventIds.generateOne.copy(projectId = projectId)
      val event1Body      = eventBodies.generateOne
      val event1BatchDate = batchDates.generateOne
      storeNewEvent(event1Id, ExecutionDate(now minus (5, SEC)), event1Body, event1BatchDate, projectPath)

      val event2Id   = compoundEventIds.generateOne.copy(projectId = projectId)
      val event2Body = eventBodies.generateOne
      storeNewEvent(event2Id, ExecutionDate(now plus (5, H)), event2Body, projectPath = projectPath)

      val event3Id        = compoundEventIds.generateOne.copy(projectId = projectId)
      val event3Body      = eventBodies.generateOne
      val event3BatchDate = batchDates.generateOne
      storeEvent(
        event3Id,
        EventStatus.RecoverableFailure,
        ExecutionDate(now minus (5, H)),
        eventDates.generateOne,
        event3Body,
        batchDate   = event3BatchDate,
        projectPath = projectPath
      )

      findEvents(EventStatus.Processing) shouldBe List.empty

      expectWaitingEventsGaugeDecrement(projectPath)
      expectUnderProcessingGaugeIncrement(projectPath)

      eventLogFetch.popEvent.unsafeRunSync() shouldBe Some(event3Id -> event3Body)

      findEvents(EventStatus.Processing) shouldBe List((event3Id, executionDate, event3BatchDate))

      expectWaitingEventsGaugeDecrement(projectPath)
      expectUnderProcessingGaugeIncrement(projectPath)

      eventLogFetch.popEvent.unsafeRunSync() shouldBe Some(event1Id -> event1Body)

      findEvents(EventStatus.Processing) shouldBe List((event1Id, executionDate, event1BatchDate),
                                                       (event3Id, executionDate, event3BatchDate))

      eventLogFetch.popEvent.unsafeRunSync() shouldBe None
    }

    s"return an event with the $Processing status " +
      "and execution date older than RenkuLogTimeout + 5 min" in new TestCase {

      val projectPath    = projectPaths.generateOne
      val eventId        = compoundEventIds.generateOne
      val eventBody      = eventBodies.generateOne
      val eventBatchDate = batchDates.generateOne
      storeEvent(
        eventId,
        EventStatus.Processing,
        ExecutionDate(now minus (maxProcessingTime.toMinutes + 5, MIN)),
        eventDates.generateOne,
        eventBody,
        batchDate   = eventBatchDate,
        projectPath = projectPath
      )

      expectWaitingEventsGaugeDecrement(projectPath)
      expectUnderProcessingGaugeIncrement(projectPath)

      eventLogFetch.popEvent.unsafeRunSync() shouldBe Some(eventId -> eventBody)

      findEvents(EventStatus.Processing) shouldBe List((eventId, executionDate, eventBatchDate))
    }

    s"return no event when there there's one with $Processing status " +
      "but execution date from less than RenkuLogTimeout + 1 min" in new TestCase {

      storeEvent(
        compoundEventIds.generateOne,
        EventStatus.Processing,
        ExecutionDate(now minus (maxProcessingTime minusSeconds 1)),
        eventDates.generateOne,
        eventBodies.generateOne
      )

      eventLogFetch.popEvent.unsafeRunSync() shouldBe None
    }

    "return no events when there are no events matching the criteria" in new TestCase {

      storeNewEvent(
        compoundEventIds.generateOne,
        ExecutionDate(now plus (50, H)),
        eventBodies.generateOne
      )

      eventLogFetch.popEvent.unsafeRunSync() shouldBe None
    }

    "return events from various projects " +
      "even if some projects have events with execution dates far in the past" in new TestCase {

      val allProjectIds = nonEmptyList(projectIds, minElements = 2).generateOne
      val eventIdsBodiesDates = for {
        projectId     <- allProjectIds
        eventId       <- nonEmptyList(eventIds, minElements = 5).generateOne map (CompoundEventId(_, projectId))
        eventBody     <- nonEmptyList(eventBodies, maxElements = 1).generateOne
        executionDate <- NonEmptyList.of(executionDateDifferentiated(by = projectId, allProjectIds))
      } yield (eventId, executionDate, eventBody)

      eventIdsBodiesDates.toList foreach {
        case (eventId, eventExecutionDate, eventBody) => storeNewEvent(eventId, eventExecutionDate, eventBody)
      }

      findEvents(EventStatus.Processing) shouldBe List.empty

      (waitingEventsGauge.decrement _).expects(*).returning(IO.unit).repeat(eventIdsBodiesDates.size)
      (underProcessingGauge.increment _).expects(*).returning(IO.unit).repeat(eventIdsBodiesDates.size)

      override val eventLogFetch = new EventFetcherImpl(
        transactor,
        renkuLogTimeout,
        waitingEventsGauge,
        underProcessingGauge
      )
      eventIdsBodiesDates.toList foreach { _ =>
        eventLogFetch.popEvent.unsafeRunSync() shouldBe a[Some[_]]
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

      val project1Id = projectIds.generateOne
      storeEvent(
        CompoundEventId(eventIds.generateOne, project1Id),
        EventStatus.Processing,
        ExecutionDate(now minus (maxProcessingTime.toMinutes + 1, MIN)),
        eventDates.generateOne,
        eventBodies.generateOne
      )
      val project2Id   = projectIds.generateOne
      val project2Path = projectPaths.generateOne
      storeEvent(
        CompoundEventId(eventIds.generateOne, project2Id),
        EventStatus.New,
        ExecutionDate(now minus (1, MIN)),
        eventDates.generateOne,
        eventBodies.generateOne
      )

      findEvents(EventStatus.Processing).map(_._1.projectId) shouldBe List(project1Id)

      override val eventLogFetch = new EventFetcherImpl(
        transactor,
        renkuLogTimeout,
        waitingEventsGauge,
        underProcessingGauge,
        pickRandomlyFrom = _ => (project2Id -> project2Path).some
      )
      expectWaitingEventsGaugeDecrement(project2Path)
      expectUnderProcessingGaugeIncrement(project2Path)
      eventLogFetch.popEvent.unsafeRunSync() shouldBe a[Some[_]]

      findEvents(
        status  = Processing,
        orderBy = fr"execution_date asc"
      ).map(_._1.projectId) should contain theSameElementsAs List(project1Id, project2Id)
    }
  }

  private trait TestCase {

    val currentTime          = mockFunction[Instant]
    val renkuLogTimeout      = renkuLogTimeouts.generateOne
    val waitingEventsGauge   = mock[LabeledGauge[IO, Path]]
    val underProcessingGauge = mock[LabeledGauge[IO, Path]]
    val maxProcessingTime    = renkuLogTimeout.toUnsafe[Duration] plusMinutes 5
    val eventLogFetch = new EventFetcherImpl(
      transactor,
      renkuLogTimeout,
      waitingEventsGauge,
      underProcessingGauge,
      currentTime
    )

    val now           = Instant.now()
    val executionDate = ExecutionDate(now)
    currentTime.expects().returning(now).anyNumberOfTimes()

    def executionDateDifferentiated(by: Id, allProjects: NonEmptyList[Id]) =
      ExecutionDate(now minus (1000 - (allProjects.toList.indexOf(by) * 10), SEC))

    def expectWaitingEventsGaugeDecrement(projectPath: Path) =
      (waitingEventsGauge.decrement _)
        .expects(projectPath)
        .returning(IO.unit)

    def expectUnderProcessingGaugeIncrement(projectPath: Path) =
      (underProcessingGauge.increment _)
        .expects(projectPath)
        .returning(IO.unit)
  }

  private def storeNewEvent(commitEventId: CompoundEventId,
                            executionDate: ExecutionDate,
                            eventBody:     EventBody,
                            batchDate:     BatchDate = batchDates.generateOne,
                            projectPath:   Path = projectPaths.generateOne): Unit =
    storeEvent(commitEventId,
               New,
               executionDate,
               eventDates.generateOne,
               eventBody,
               batchDate   = batchDate,
               projectPath = projectPath)
}
