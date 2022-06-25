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

package io.renku.eventlog.events.producers
package awaitinggeneration

import ProjectPrioritisation.Priority.MaxPriority
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog._
import io.renku.eventlog.events.producers.awaitinggeneration.ProjectPrioritisation.{Priority, ProjectInfo}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{BatchDate, CompoundEventId, EventBody, EventStatus}
import io.renku.graph.model.projects.{Id, Path}
import io.renku.metrics.{LabeledGauge, TestLabeledHistogram}
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import java.time.temporal.ChronoUnit.DAYS

private class EventFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with should.Matchers {

  "popEvent" should {

    s"find the most recent event in status $New or $GenerationRecoverableFailure " +
      s"and mark it as $GeneratingTriples" in new TestCase {
        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne

        val (_, _, notLatestEventDate, _) = createEvent(
          status = Gen.oneOf(New, GenerationRecoverableFailure).generateOne,
          eventDate = timestamps(max = now.minus(2, DAYS)).generateAs(EventDate),
          projectId = projectId,
          projectPath = projectPath
        )

        val (event2Id, event2Body, latestEventDate, _) = createEvent(
          status = Gen.oneOf(New, GenerationRecoverableFailure).generateOne,
          eventDate = timestamps(min = notLatestEventDate.value, max = now).generateAs(EventDate),
          projectId = projectId,
          projectPath = projectPath
        )

        findEvents(EventStatus.GeneratingTriples) shouldBe List.empty

        expectWaitingEventsGaugeDecrement(projectPath)
        expectUnderProcessingGaugeIncrement(projectPath)

        givenPrioritisation(
          takes = List(ProjectInfo(projectId, projectPath, latestEventDate, currentOccupancy = 0)),
          totalOccupancy = 0,
          returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
        )

        finder.popEvent().unsafeRunSync() shouldBe Some(
          AwaitingGenerationEvent(event2Id, projectPath, event2Body)
        )

        findEvents(EventStatus.GeneratingTriples).noBatchDate shouldBe List((event2Id, executionDate))

        givenPrioritisation(takes = Nil, totalOccupancy = 1, returns = Nil)

        finder.popEvent().unsafeRunSync() shouldBe None

        queriesExecTimes.verifyExecutionTimeMeasured("awaiting_generation - find projects",
                                                     "awaiting_generation - find latest",
                                                     "awaiting_generation - update status"
        )
      }

    s"find the most recent event in status $New or $GenerationRecoverableFailure " +
      "if there are multiple latest events with the same date" in new TestCase {
        val projectId       = projectIds.generateOne
        val projectPath     = projectPaths.generateOne
        val latestEventDate = eventDates.generateOne

        val (event1Id, event1Body, _, _) = createEvent(
          status = Gen.oneOf(New, GenerationRecoverableFailure).generateOne,
          eventDate = latestEventDate,
          projectId = projectId,
          projectPath = projectPath
        )

        val (event2Id, event2Body, _, _) = createEvent(
          status = Gen.oneOf(New, GenerationRecoverableFailure).generateOne,
          eventDate = latestEventDate,
          projectId = projectId,
          projectPath = projectPath
        )

        findEvents(EventStatus.GeneratingTriples) shouldBe List.empty

        // 1st event with the same event date
        expectWaitingEventsGaugeDecrement(projectPath)
        expectUnderProcessingGaugeIncrement(projectPath)

        givenPrioritisation(
          takes = List(ProjectInfo(projectId, projectPath, latestEventDate, currentOccupancy = 0)),
          totalOccupancy = 0,
          returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
        )

        finder.popEvent().unsafeRunSync() should {
          be(AwaitingGenerationEvent(event1Id, projectPath, event1Body).some) or
            be(AwaitingGenerationEvent(event2Id, projectPath, event2Body).some)
        }

        findEvents(EventStatus.GeneratingTriples).noBatchDate should {
          be(List((event1Id, executionDate))) or be(List((event2Id, executionDate)))
        }

        // 2nd event with the same event date
        expectWaitingEventsGaugeDecrement(projectPath)
        expectUnderProcessingGaugeIncrement(projectPath)

        givenPrioritisation(
          takes = List(ProjectInfo(projectId, projectPath, latestEventDate, currentOccupancy = 1)),
          totalOccupancy = 1,
          returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
        )

        finder.popEvent().unsafeRunSync() should {
          be(AwaitingGenerationEvent(event1Id, projectPath, event1Body).some) or
            be(AwaitingGenerationEvent(event2Id, projectPath, event2Body).some)
        }

        findEvents(EventStatus.GeneratingTriples).noBatchDate should contain theSameElementsAs List(
          event1Id -> executionDate,
          event2Id -> executionDate
        )

        // no more events left
        givenPrioritisation(takes = Nil, totalOccupancy = 2, returns = Nil)

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    Set(GenerationNonRecoverableFailure, TransformationNonRecoverableFailure, Skipped) foreach { latestEventStatus =>
      s"find the most recent event in status $New or $GenerationRecoverableFailure " +
        s"if the latest event is in status $latestEventStatus" in new TestCase {
          val projectId   = projectIds.generateOne
          val projectPath = projectPaths.generateOne

          val (event1Id, event1Body, notLatestEventDate, _) = createEvent(
            status = Gen.oneOf(New, GenerationRecoverableFailure).generateOne,
            eventDate = timestamps(max = now.minus(2, DAYS)).generateAs(EventDate),
            projectId = projectId,
            projectPath = projectPath
          )

          val (_, _, latestEventDate, _) = createEvent(
            status = latestEventStatus,
            eventDate = timestamps(min = notLatestEventDate.value, max = now).generateAs(EventDate),
            projectId = projectId,
            projectPath = projectPath
          )

          findEvents(EventStatus.GeneratingTriples) shouldBe List.empty

          expectWaitingEventsGaugeDecrement(projectPath)
          expectUnderProcessingGaugeIncrement(projectPath)

          givenPrioritisation(
            takes = List(ProjectInfo(projectId, projectPath, latestEventDate, currentOccupancy = 0)),
            totalOccupancy = 0,
            returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
          )

          finder.popEvent().unsafeRunSync() shouldBe Some(
            AwaitingGenerationEvent(event1Id, projectPath, event1Body)
          )
        }
    }

    List(GeneratingTriples,
         TriplesGenerated,
         TransformationRecoverableFailure,
         TriplesStore,
         AwaitingDeletion,
         Deleting
    ) foreach { latestEventStatus =>
      s"find no event when there are older statuses in status $New or $GenerationRecoverableFailure " +
        s"but the latest event is $latestEventStatus" in new TestCase {

          val projectId   = projectIds.generateOne
          val projectPath = projectPaths.generateOne

          val (_, _, notLatestEventDate, _) = createEvent(
            status = Gen.oneOf(New, GenerationRecoverableFailure).generateOne,
            eventDate = timestamps(max = now.minus(2, DAYS)).generateAs(EventDate),
            projectId = projectId,
            projectPath = projectPath
          )

          createEvent(
            status = latestEventStatus,
            eventDate = timestamps(min = notLatestEventDate.value, max = now).generateAs(EventDate),
            projectId = projectId,
            projectPath = projectPath
          )

          givenPrioritisation(takes = Nil,
                              totalOccupancy = if (latestEventStatus == GeneratingTriples) 1 else 0,
                              returns = Nil
          )

          finder.popEvent().unsafeRunSync() shouldBe None
        }
    }

    "find no event when execution date is in the future " +
      s"and status $New or $GenerationRecoverableFailure " in new TestCase {

        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne

        val (_, _, event1Date, _) = createEvent(
          status = New,
          eventDate = timestamps(max = Instant.now().minusSeconds(5)).generateAs(EventDate),
          projectId = projectId,
          projectPath = projectPath
        )

        val (_, _, event2Date, _) = createEvent(
          status = GenerationRecoverableFailure,
          eventDate = EventDate(event1Date.value plusSeconds 1),
          executionDate = timestampsInTheFuture.generateAs(ExecutionDate),
          projectId = projectId,
          projectPath = projectPath
        )

        createEvent(
          status = New,
          eventDate = EventDate(event2Date.value plusSeconds 1),
          executionDate = timestampsInTheFuture.generateAs(ExecutionDate),
          projectId = projectId,
          projectPath = projectPath
        )

        findEvents(EventStatus.GeneratingTriples) shouldBe List.empty

        givenPrioritisation(takes = Nil, totalOccupancy = 0, returns = Nil)

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "find the latest events from each project" in new TestCase {

      val events = readyStatuses
        .generateNonEmptyList(minElements = 2)
        .map(createEvent(_))
        .toList

      findEvents(EventStatus.GeneratingTriples) shouldBe List.empty

      expectGaugeUpdated(times = events.size)

      events.sortBy(_._3).reverse.zipWithIndex foreach { case ((eventId, _, eventDate, projectPath), index) =>
        givenPrioritisation(
          takes = List(ProjectInfo(eventId.projectId, projectPath, eventDate, 0)),
          totalOccupancy = index,
          returns = List(ProjectIds(eventId.projectId, projectPath) -> MaxPriority)
        )
      }

      events foreach { _ =>
        finder.popEvent().unsafeRunSync() shouldBe a[Some[_]]
      }

      findEvents(status = GeneratingTriples).eventIdsOnly should contain theSameElementsAs events.map(_._1)
    }

    "return the latest events from each projects - case with projectsFetchingLimit > 1" in new TestCaseCommons {

      val eventLogFind = new EventFinderImpl(waitingEventsGauge,
                                             underProcessingGauge,
                                             queriesExecTimes,
                                             currentTime,
                                             projectsFetchingLimit = 5,
                                             projectPrioritisation = projectPrioritisation
      )

      val events = readyStatuses
        .generateNonEmptyList(minElements = 3, maxElements = 6)
        .toList
        .flatMap { status =>
          (1 to positiveInts(max = 2).generateOne.value)
            .map(_ => createEvent(status, projectId = projectIds.generateOne, projectPath = projectPaths.generateOne))
        }

      findEvents(EventStatus.GeneratingTriples) shouldBe List.empty

      val eventsPerProject = events.groupBy(_._4).toList.sortBy(_._2.maxBy(_._3)._3).reverse

      expectGaugeUpdated(times = eventsPerProject.size)

      eventsPerProject.zipWithIndex foreach { case ((projectPath, events), index) =>
        val (eventIdOfTheNewestEvent, _, _, _) = events.sortBy(_._3).reverse.head
        (projectPrioritisation.prioritise _)
          .expects(*, index)
          .returning(List(ProjectIds(eventIdOfTheNewestEvent.projectId, projectPath) -> MaxPriority))
      }

      eventsPerProject foreach { _ =>
        eventLogFind.popEvent().unsafeRunSync() shouldBe a[Some[_]]
      }

      findEvents(status = GeneratingTriples).eventIdsOnly should contain theSameElementsAs eventsPerProject.map {
        case (_, list) => list.maxBy(_._3)._1
      }

      givenPrioritisation(takes = Nil, totalOccupancy = eventsPerProject.size, returns = Nil)

      eventLogFind.popEvent().unsafeRunSync() shouldBe None
    }
  }

  private trait TestCaseCommons {
    val now           = Instant.now()
    val executionDate = ExecutionDate(now)
    val currentTime   = mockFunction[Instant]
    currentTime.expects().returning(now).anyNumberOfTimes()

    val waitingEventsGauge    = mock[LabeledGauge[IO, Path]]
    val underProcessingGauge  = mock[LabeledGauge[IO, Path]]
    val projectPrioritisation = mock[ProjectPrioritisation[IO]]
    val queriesExecTimes      = TestLabeledHistogram[SqlStatement.Name]("query_id")

    def expectWaitingEventsGaugeDecrement(projectPath: Path) =
      (waitingEventsGauge.decrement _)
        .expects(projectPath)
        .returning(IO.unit)

    def expectUnderProcessingGaugeIncrement(projectPath: Path) =
      (underProcessingGauge.increment _)
        .expects(projectPath)
        .returning(IO.unit)

    def expectGaugeUpdated(times: Int = 1) = {
      (waitingEventsGauge.decrement _).expects(*).returning(IO.unit).repeat(times)
      (underProcessingGauge.increment _).expects(*).returning(IO.unit).repeat(times)
    }

    def givenPrioritisation(takes: List[ProjectInfo], totalOccupancy: Long, returns: List[(ProjectIds, Priority)]) =
      (projectPrioritisation.prioritise _)
        .expects(takes, totalOccupancy)
        .returning(returns)
  }

  private trait TestCase extends TestCaseCommons {

    val finder = new EventFinderImpl(waitingEventsGauge,
                                     underProcessingGauge,
                                     queriesExecTimes,
                                     currentTime,
                                     projectsFetchingLimit = 1,
                                     projectPrioritisation = projectPrioritisation
    )
  }

  private def executionDatesInThePast: Gen[ExecutionDate] = timestampsNotInTheFuture map ExecutionDate.apply

  private def readyStatuses = Gen.oneOf(New, GenerationRecoverableFailure)

  private def createEvent(status:        EventStatus,
                          eventDate:     EventDate = eventDates.generateOne,
                          executionDate: ExecutionDate = executionDatesInThePast.generateOne,
                          batchDate:     BatchDate = batchDates.generateOne,
                          projectId:     Id = projectIds.generateOne,
                          projectPath:   Path = projectPaths.generateOne
  ): (CompoundEventId, EventBody, EventDate, Path) = {
    val eventId   = compoundEventIds.generateOne.copy(projectId = projectId)
    val eventBody = eventBodies.generateOne

    storeEvent(eventId, status, executionDate, eventDate, eventBody, batchDate = batchDate, projectPath = projectPath)

    (eventId, eventBody, eventDate, projectPath)
  }
}
