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

package io.renku.eventlog.subscriptions.awaitinggeneration

import java.time.temporal.ChronoUnit.{HOURS => H}
import java.time.Instant

import cats.effect.IO
import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{BatchDate, CompoundEventId, EventBody, EventStatus}
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.metrics.{LabeledGauge, TestLabeledHistogram}
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog._
import io.renku.eventlog.subscriptions.ProjectIds
import io.renku.eventlog.subscriptions.awaitinggeneration.ProjectPrioritisation.Priority.MaxPriority
import io.renku.eventlog.subscriptions.awaitinggeneration.ProjectPrioritisation.{Priority, ProjectInfo}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

private class AwaitingGenerationEventFinderSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with should.Matchers {

  "popEvent" should {

    "return an event with event date farthest in the past " +
      s"and status $New or $GenerationRecoverableFailure " +
      s"and mark it as $GeneratingTriples" in new TestCase {

        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne

        val (event1Id, event1Body, latestEventDate, _) = createEvent(
          status = New,
          eventDate = EventDate(now.minus(1, H)),
          projectId = projectId,
          projectPath = projectPath
        )

        val (event2Id, event2Body, _, _) = createEvent(
          status = EventStatus.GenerationRecoverableFailure,
          EventDate(now.minus(5, H)),
          projectId = projectId,
          projectPath = projectPath
        )

        findEvents(EventStatus.GeneratingTriples) shouldBe List.empty

        expectWaitingEventsGaugeDecrement(projectPath)
        expectUnderProcessingGaugeIncrement(projectPath)

        givenPrioritisation(
          takes = List(ProjectInfo(projectId, projectPath, latestEventDate, 0)),
          totalOccupancy = 0,
          returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
        )

        finder.popEvent().unsafeRunSync() shouldBe Some(
          AwaitingGenerationEvent(event2Id, projectPath, event2Body)
        )

        findEvents(EventStatus.GeneratingTriples).noBatchDate shouldBe List((event2Id, executionDate))

        expectWaitingEventsGaugeDecrement(projectPath)
        expectUnderProcessingGaugeIncrement(projectPath)

        givenPrioritisation(
          takes = List(ProjectInfo(projectId, projectPath, latestEventDate, 1)),
          totalOccupancy = 1,
          returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
        )

        finder.popEvent().unsafeRunSync() shouldBe Some(
          AwaitingGenerationEvent(event1Id, projectPath, event1Body)
        )

        findEvents(EventStatus.GeneratingTriples).noBatchDate shouldBe List((event1Id, executionDate),
                                                                            (event2Id, executionDate)
        )

        givenPrioritisation(takes = Nil, totalOccupancy = 2, returns = Nil)

        finder.popEvent().unsafeRunSync() shouldBe None

        queriesExecTimes.verifyExecutionTimeMeasured("awaiting_generation - find projects",
                                                     "awaiting_generation - find oldest",
                                                     "awaiting_generation - update status"
        )
      }

    "return no event when execution date is in the future " +
      s"and status $New or $GenerationRecoverableFailure " in new TestCase {

        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne

        val (event1Id, event1Body, event1Date, _) = createEvent(
          status = New,
          projectId = projectId,
          projectPath = projectPath
        )

        val (_, _, event2Date, _) = createEvent(
          status = GenerationRecoverableFailure,
          executionDate = ExecutionDate(timestampsInTheFuture.generateOne),
          projectId = projectId,
          projectPath = projectPath
        )

        val (_, _, event3Date, _) = createEvent(
          status = New,
          executionDate = ExecutionDate(timestampsInTheFuture.generateOne),
          projectId = projectId,
          projectPath = projectPath
        )

        findEvents(EventStatus.GeneratingTriples) shouldBe List.empty

        expectWaitingEventsGaugeDecrement(projectPath)
        expectUnderProcessingGaugeIncrement(projectPath)

        val latestEventDate = List(event1Date, event2Date, event3Date).max
        givenPrioritisation(
          takes = List(ProjectInfo(projectId, projectPath, latestEventDate, 0)),
          totalOccupancy = 0,
          returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
        )

        finder.popEvent().unsafeRunSync() shouldBe Some(
          AwaitingGenerationEvent(event1Id, projectPath, event1Body)
        )

        findEvents(EventStatus.GeneratingTriples).noBatchDate shouldBe List((event1Id, executionDate))

        givenPrioritisation(takes = Nil, totalOccupancy = 1, returns = Nil)

        finder.popEvent().unsafeRunSync() shouldBe None

        queriesExecTimes.verifyExecutionTimeMeasured("awaiting_generation - find projects",
                                                     "awaiting_generation - find oldest",
                                                     "awaiting_generation - update status"
        )
      }

    "return events from all the projects" in new TestCase {

      val events = readyStatuses
        .generateNonEmptyList(minElements = 2)
        .map(status => createEvent(status))
        .toList

      findEvents(EventStatus.GeneratingTriples) shouldBe List.empty

      expectGaugeUpdated(times = events.size)

      events foreach { case (eventId, _, eventDate, projectPath) =>
        givenPrioritisation(
          takes = List(ProjectInfo(eventId.projectId, projectPath, eventDate, 0)),
          totalOccupancy = 0,
          returns = List(ProjectIds(eventId.projectId, projectPath) -> MaxPriority)
        )
      }

      events foreach { _ =>
        finder.popEvent().unsafeRunSync() shouldBe a[Some[_]]
      }

      findEvents(status = GeneratingTriples).eventIdsOnly should contain theSameElementsAs events.map(_._1)
    }

    "return events from all the projects - case with projectsFetchingLimit > 1" in new TestCaseCommons {

      val eventLogFind = new AwaitingGenerationEventFinderImpl(
        sessionResource,
        waitingEventsGauge,
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
          val projectId   = projectIds.generateOne
          val projectPath = projectPaths.generateOne
          (1 to positiveInts(max = 2).generateOne.value)
            .map(_ => createEvent(status, projectId = projectId, projectPath = projectPath))
        }

      findEvents(EventStatus.GeneratingTriples) shouldBe List.empty

      expectGaugeUpdated(times = events.size)

      events foreach { case (eventId, _, _, projectPath) =>
        (projectPrioritisation.prioritise _)
          .expects(*, 0)
          .returning(List(ProjectIds(eventId.projectId, projectPath) -> MaxPriority))
      }

      events foreach { _ =>
        eventLogFind.popEvent().unsafeRunSync() shouldBe a[Some[_]]
      }

      findEvents(status = GeneratingTriples).eventIdsOnly should contain theSameElementsAs events.map(_._1)

      givenPrioritisation(takes = Nil, totalOccupancy = events.size, returns = Nil)

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

    val finder = new AwaitingGenerationEventFinderImpl(
      sessionResource,
      waitingEventsGauge,
      underProcessingGauge,
      queriesExecTimes,
      currentTime,
      projectsFetchingLimit = 1,
      projectPrioritisation = projectPrioritisation
    )
  }

  private def executionDatesInThePast: Gen[ExecutionDate] = timestampsNotInTheFuture map ExecutionDate.apply

  private def readyStatuses = Gen
    .oneOf(EventStatus.New, EventStatus.GenerationRecoverableFailure)

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
