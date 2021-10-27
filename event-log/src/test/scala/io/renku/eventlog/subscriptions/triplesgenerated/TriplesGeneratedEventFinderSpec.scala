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

package io.renku.eventlog.subscriptions.triplesgenerated

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog._
import io.renku.eventlog.subscriptions.ProjectIds
import io.renku.eventlog.subscriptions.triplesgenerated.ProjectPrioritisation.Priority.MaxPriority
import io.renku.eventlog.subscriptions.triplesgenerated.ProjectPrioritisation.{Priority, ProjectInfo}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{BatchDate, CompoundEventId, EventBody, EventStatus, ZippedEventPayload}
import io.renku.graph.model.projects.{Id, Path}
import io.renku.metrics.{LabeledGauge, TestLabeledHistogram}
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

private class TriplesGeneratedEventFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with should.Matchers {

  "popEvent" should {

    "return an event with the latest event date " +
      s"and status $TriplesGenerated or $TransformationRecoverableFailure " +
      s"and mark it as $TransformingTriples" in new TestCase {

        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne

        val (event1Id, _, latestEventDate, _, eventPayload1) = createEvent(
          status = TriplesGenerated,
          eventDate = timestampsNotInTheFuture.generateAs(EventDate),
          projectId = projectId,
          projectPath = projectPath
        )

        createEvent(
          status = TransformationRecoverableFailure,
          timestamps(max = latestEventDate.value).generateAs(EventDate),
          projectId = projectId,
          projectPath = projectPath
        )

        findEvents(TransformingTriples) shouldBe List.empty

        expectAwaitingTransformationGaugeDecrement(projectPath)
        expectUnderTransformationGaugeIncrement(projectPath)

        givenPrioritisation(
          takes = List(ProjectInfo(projectId, projectPath, latestEventDate, 0)),
          returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
        )

        val Some(TriplesGeneratedEvent(actualEventId, actualPath, actualPayload)) = finder.popEvent().unsafeRunSync()
        actualEventId     shouldBe event1Id
        actualPath        shouldBe projectPath
        actualPayload.value should contain theSameElementsAs eventPayload1.value

        findEvents(TransformingTriples).noBatchDate shouldBe List((event1Id, executionDate))

        givenPrioritisation(takes = Nil, returns = Nil)

        finder.popEvent().unsafeRunSync() shouldBe None

        findEvents(TransformingTriples).noBatchDate shouldBe List((event1Id, executionDate))

        queriesExecTimes.verifyExecutionTimeMeasured("triples_generated - find projects",
                                                     "triples_generated - find oldest",
                                                     "triples_generated - update status"
        )
      }

    "return an event with the latest event date " +
      s"and status $TriplesGenerated or $TransformationRecoverableFailure " +
      s"and mark it as $TransformingTriples " +
      s"case - when a newer event arrive after the pop" in new TestCase {

        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne

        val (event1Id, _, latestEventDate, _, eventPayload1) = createEvent(
          status = TriplesGenerated,
          eventDate = timestampsNotInTheFuture.generateAs(EventDate),
          projectId = projectId,
          projectPath = projectPath
        )

        findEvents(TransformingTriples) shouldBe List.empty

        expectAwaitingTransformationGaugeDecrement(projectPath)
        expectUnderTransformationGaugeIncrement(projectPath)

        givenPrioritisation(
          takes = List(ProjectInfo(projectId, projectPath, latestEventDate, 0)),
          returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
        )

        val Some(TriplesGeneratedEvent(actualEvent1Id, actualEvent1Path, actualEvent1Payload)) =
          finder.popEvent().unsafeRunSync()
        actualEvent1Id          shouldBe event1Id
        actualEvent1Path        shouldBe projectPath
        actualEvent1Payload.value should contain theSameElementsAs eventPayload1.value

        findEvents(TransformingTriples).noBatchDate shouldBe List((event1Id, executionDate))

        val (event2Id, _, newerLatestEventDate, _, eventPayload2) = createEvent(
          status = TransformationRecoverableFailure,
          timestamps(min = latestEventDate.value, max = now).generateAs(EventDate),
          projectId = projectId,
          projectPath = projectPath
        )

        expectAwaitingTransformationGaugeDecrement(projectPath)
        expectUnderTransformationGaugeIncrement(projectPath)

        givenPrioritisation(
          takes = List(ProjectInfo(projectId, projectPath, newerLatestEventDate, 1)),
          returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
        )

        val Some(TriplesGeneratedEvent(actualEvent2Id, actualEvent2Path, actualEvent2Payload)) =
          finder.popEvent().unsafeRunSync()
        actualEvent2Id          shouldBe event2Id
        actualEvent2Path        shouldBe projectPath
        actualEvent2Payload.value should contain theSameElementsAs eventPayload2.value

        findEvents(TransformingTriples).noBatchDate shouldBe List((event1Id, executionDate), (event2Id, executionDate))

        queriesExecTimes.verifyExecutionTimeMeasured("triples_generated - find projects",
                                                     "triples_generated - find oldest",
                                                     "triples_generated - update status"
        )
      }

    s"skip events in $TriplesGenerated status which do not have payload - within a project" in new TestCase {
      val projectId   = projectIds.generateOne
      val projectPath = projectPaths.generateOne

      val event1Id   = compoundEventIds.generateOne.copy(projectId = projectId)
      val event1Date = timestampsNotInTheFuture.generateAs(EventDate)
      storeEvent(
        event1Id,
        TriplesGenerated,
        executionDatesInThePast.generateOne,
        event1Date,
        eventBodies.generateOne,
        projectPath = projectPath,
        maybeEventPayload = None
      )

      val (event2Id, _, _, _, eventPayload2) = createEvent(
        status = TriplesGenerated,
        eventDate = timestamps(max = event1Date.value).generateAs(EventDate),
        projectId = projectId,
        projectPath = projectPath
      )

      expectAwaitingTransformationGaugeDecrement(projectPath)
      expectUnderTransformationGaugeIncrement(projectPath)

      givenPrioritisation(
        takes = List(ProjectInfo(projectId, projectPath, event1Date, 0)),
        returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
      )

      val Some(TriplesGeneratedEvent(actualEventId, actualEventPath, actualEventPayload)) =
        finder.popEvent().unsafeRunSync()
      actualEventId          shouldBe event2Id
      actualEventPath        shouldBe projectPath
      actualEventPayload.value should contain theSameElementsAs eventPayload2.value

      findEvents(TransformingTriples).noBatchDate shouldBe List((event2Id, executionDate))
    }

    s"skip projects with events in $TriplesGenerated status which do not have payload" in new TestCase {
      val event1ProjectId   = projectIds.generateOne
      val event1ProjectPath = projectPaths.generateOne

      val event1Id   = compoundEventIds.generateOne.copy(projectId = event1ProjectId)
      val event1Date = timestampsNotInTheFuture.generateAs(EventDate)
      storeEvent(
        event1Id,
        TriplesGenerated,
        executionDatesInThePast.generateOne,
        event1Date,
        eventBodies.generateOne,
        projectPath = event1ProjectPath,
        maybeEventPayload = None
      )

      val event2ProjectId   = projectIds.generateOne
      val event2ProjectPath = projectPaths.generateOne
      val event2Date        = timestamps(max = event1Date.value).generateAs(EventDate)
      val (event2Id, _, _, _, eventPayload2) = createEvent(
        status = TriplesGenerated,
        eventDate = event2Date,
        projectId = event2ProjectId,
        projectPath = event2ProjectPath
      )

      expectAwaitingTransformationGaugeDecrement(event2ProjectPath)
      expectUnderTransformationGaugeIncrement(event2ProjectPath)

      givenPrioritisation(
        takes = List(ProjectInfo(event2ProjectId, event2ProjectPath, event2Date, 0)),
        returns = List(ProjectIds(event2ProjectId, event2ProjectPath) -> MaxPriority)
      )

      val Some(TriplesGeneratedEvent(actualEventId, actualEventPath, actualEventPayload)) =
        finder.popEvent().unsafeRunSync()
      actualEventId          shouldBe event2Id
      actualEventPath        shouldBe event2ProjectPath
      actualEventPayload.value should contain theSameElementsAs eventPayload2.value

      findEvents(TransformingTriples).noBatchDate shouldBe List((event2Id, executionDate))
    }

    "return no event when execution date is in the future " +
      s"and status $TriplesGenerated or $TransformationRecoverableFailure " in new TestCase {

        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne

        val (event1Id, _, event1Date, _, eventPayload1) = createEvent(
          status = TriplesGenerated,
          projectId = projectId,
          projectPath = projectPath
        )

        val (_, _, event2Date, _, _) = createEvent(
          status = TransformationRecoverableFailure,
          executionDate = ExecutionDate(timestampsInTheFuture.generateOne),
          projectId = projectId,
          projectPath = projectPath
        )

        val (_, _, event3Date, _, _) = createEvent(
          status = TriplesGenerated,
          executionDate = ExecutionDate(timestampsInTheFuture.generateOne),
          projectId = projectId,
          projectPath = projectPath
        )

        findEvents(TransformingTriples) shouldBe List.empty

        expectAwaitingTransformationGaugeDecrement(projectPath)
        expectUnderTransformationGaugeIncrement(projectPath)

        givenPrioritisation(
          takes = List(ProjectInfo(projectId, projectPath, List(event1Date, event2Date, event3Date).max, 0)),
          returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
        )

        val Some(TriplesGeneratedEvent(actualEventId, actualPath, actualPayload)) = finder.popEvent().unsafeRunSync()
        actualEventId     shouldBe event1Id
        actualPath        shouldBe projectPath
        actualPayload.value should contain theSameElementsAs eventPayload1.value

        findEvents(TransformingTriples).noBatchDate shouldBe List((event1Id, executionDate))

        givenPrioritisation(takes = Nil, returns = Nil)

        finder.popEvent().unsafeRunSync() shouldBe None

        queriesExecTimes.verifyExecutionTimeMeasured("triples_generated - find projects",
                                                     "triples_generated - find oldest",
                                                     "triples_generated - update status"
        )
      }

    "return events from all the projects" in new TestCase {

      val events = readyStatuses
        .generateNonEmptyList(minElements = 2)
        .map(status => createEvent(status))
        .toList

      findEvents(TransformingTriples) shouldBe List.empty

      expectGaugeUpdated(times = events.size)

      events foreach { case (eventId, _, eventDate, projectPath, _) =>
        givenPrioritisation(
          takes = List(ProjectInfo(eventId.projectId, projectPath, eventDate, 0)),
          returns = List(ProjectIds(eventId.projectId, projectPath) -> MaxPriority)
        )
      }

      events foreach { _ =>
        finder.popEvent().unsafeRunSync() shouldBe a[Some[_]]
      }

      findEvents(status = TransformingTriples).eventIdsOnly should contain theSameElementsAs events.map(_._1)
    }

    "return events from all the projects - case with projectsFetchingLimit > 1" in new TestCaseCommons {

      val eventLogFind = new TriplesGeneratedEventFinderImpl(
        sessionResource,
        awaitingTransformationGauge,
        underTransformationGauge,
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

      findEvents(TransformingTriples) shouldBe List.empty

      val eventsGroupedByProjects = events.groupBy(_._1.projectId)

      expectGaugeUpdated(times = eventsGroupedByProjects.size)

      eventsGroupedByProjects foreach {
        case (projectId, (_, _, _, projectPath, _) :: _) =>
          (projectPrioritisation.prioritise _)
            .expects(*)
            .returning(List(ProjectIds(projectId, projectPath) -> MaxPriority))
        case (_, Nil) => ()
      }

      eventsGroupedByProjects foreach { _ =>
        eventLogFind.popEvent().unsafeRunSync() shouldBe a[Some[_]]
      }

      findEvents(TransformingTriples).eventIdsOnly should contain theSameElementsAs eventsGroupedByProjects.map {
        case (_, projectEvents) => projectEvents.maxBy(_._3)._1
      }.toList

      givenPrioritisation(takes = Nil, returns = Nil)

      eventLogFind.popEvent().unsafeRunSync() shouldBe None
    }
  }

  private trait TestCaseCommons {
    val now           = Instant.now()
    val executionDate = ExecutionDate(now)
    val currentTime   = mockFunction[Instant]
    currentTime.expects().returning(now).anyNumberOfTimes()

    val awaitingTransformationGauge = mock[LabeledGauge[IO, Path]]
    val underTransformationGauge    = mock[LabeledGauge[IO, Path]]
    val projectPrioritisation       = mock[ProjectPrioritisation]
    val queriesExecTimes            = TestLabeledHistogram[SqlStatement.Name]("query_id")

    def expectAwaitingTransformationGaugeDecrement(projectPath: Path) =
      (awaitingTransformationGauge.decrement _)
        .expects(projectPath)
        .returning(IO.unit)

    def expectUnderTransformationGaugeIncrement(projectPath: Path) =
      (underTransformationGauge.increment _)
        .expects(projectPath)
        .returning(IO.unit)

    def expectGaugeUpdated(times: Int = 1) = {
      (awaitingTransformationGauge.decrement _).expects(*).returning(IO.unit).repeat(times)
      (underTransformationGauge.increment _).expects(*).returning(IO.unit).repeat(times)
    }

    def givenPrioritisation(takes: List[ProjectInfo], returns: List[(ProjectIds, Priority)]) =
      (projectPrioritisation.prioritise _)
        .expects(takes)
        .returning(returns)
  }

  private trait TestCase extends TestCaseCommons {
    val finder = new TriplesGeneratedEventFinderImpl(
      sessionResource,
      awaitingTransformationGauge,
      underTransformationGauge,
      queriesExecTimes,
      currentTime,
      projectsFetchingLimit = 1,
      projectPrioritisation = projectPrioritisation
    )
  }

  private def executionDatesInThePast: Gen[ExecutionDate] = timestampsNotInTheFuture map ExecutionDate.apply

  private def readyStatuses = Gen
    .oneOf(EventStatus.TriplesGenerated, EventStatus.TransformationRecoverableFailure)

  private def createEvent(status:        EventStatus,
                          eventDate:     EventDate = eventDates.generateOne,
                          executionDate: ExecutionDate = executionDatesInThePast.generateOne,
                          batchDate:     BatchDate = batchDates.generateOne,
                          projectId:     Id = projectIds.generateOne,
                          projectPath:   Path = projectPaths.generateOne,
                          eventPayload:  ZippedEventPayload = zippedEventPayloads.generateOne
  ): (CompoundEventId, EventBody, EventDate, Path, ZippedEventPayload) = {
    val eventId   = compoundEventIds.generateOne.copy(projectId = projectId)
    val eventBody = eventBodies.generateOne

    storeEvent(
      eventId,
      status,
      executionDate,
      eventDate,
      eventBody,
      batchDate = batchDate,
      projectPath = projectPath,
      maybeEventPayload = eventPayload.some
    )

    (eventId, eventBody, eventDate, projectPath, eventPayload)
  }
}
