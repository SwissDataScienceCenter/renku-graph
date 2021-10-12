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
import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{BatchDate, CompoundEventId, EventBody, EventStatus}
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.metrics.{LabeledGauge, TestLabeledHistogram}
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog._
import io.renku.eventlog.subscriptions.ProjectIds
import io.renku.eventlog.subscriptions.triplesgenerated.ProjectPrioritisation.Priority.MaxPriority
import io.renku.eventlog.subscriptions.triplesgenerated.ProjectPrioritisation.{Priority, ProjectInfo}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

private class TriplesGeneratedEventFinderSpec
    extends AnyWordSpec
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
          projectPath = projectPath,
          payloadSchemaVersion = schemaVersion
        )

        createEvent(
          status = TransformationRecoverableFailure,
          timestamps(max = latestEventDate.value).generateAs(EventDate),
          projectId = projectId,
          projectPath = projectPath,
          payloadSchemaVersion = schemaVersion
        )

        findEvents(TransformingTriples) shouldBe List.empty

        expectAwaitingTransformationGaugeDecrement(projectPath)
        expectUnderTransformationGaugeIncrement(projectPath)

        givenPrioritisation(
          takes = List(ProjectInfo(projectId, projectPath, latestEventDate, 0)),
          returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
        )

        finder.popEvent().unsafeRunSync() shouldBe Some(
          TriplesGeneratedEvent(event1Id, projectPath, eventPayload1, schemaVersion)
        )

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
          projectPath = projectPath,
          payloadSchemaVersion = schemaVersion
        )

        findEvents(TransformingTriples) shouldBe List.empty

        expectAwaitingTransformationGaugeDecrement(projectPath)
        expectUnderTransformationGaugeIncrement(projectPath)

        givenPrioritisation(
          takes = List(ProjectInfo(projectId, projectPath, latestEventDate, 0)),
          returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
        )

        finder.popEvent().unsafeRunSync() shouldBe Some(
          TriplesGeneratedEvent(event1Id, projectPath, eventPayload1, schemaVersion)
        )

        findEvents(TransformingTriples).noBatchDate shouldBe List((event1Id, executionDate))

        val (event2Id, _, newerLatestEventDate, _, eventPayload2) = createEvent(
          status = TransformationRecoverableFailure,
          timestamps(min = latestEventDate.value, max = now).generateAs(EventDate),
          projectId = projectId,
          projectPath = projectPath,
          payloadSchemaVersion = schemaVersion
        )

        expectAwaitingTransformationGaugeDecrement(projectPath)
        expectUnderTransformationGaugeIncrement(projectPath)

        givenPrioritisation(
          takes = List(ProjectInfo(projectId, projectPath, newerLatestEventDate, 1)),
          returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
        )

        finder.popEvent().unsafeRunSync() shouldBe Some(
          TriplesGeneratedEvent(event2Id, projectPath, eventPayload2, schemaVersion)
        )

        findEvents(TransformingTriples).noBatchDate shouldBe List((event1Id, executionDate), (event2Id, executionDate))

        queriesExecTimes.verifyExecutionTimeMeasured("triples_generated - find projects",
                                                     "triples_generated - find oldest",
                                                     "triples_generated - update status"
        )
      }

    "return no event when execution date is in the future " +
      s"and status $TriplesGenerated or $TransformationRecoverableFailure " in new TestCase {

        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne

        val (event1Id, _, event1Date, _, eventPayload1) = createEvent(
          status = TriplesGenerated,
          projectId = projectId,
          projectPath = projectPath,
          payloadSchemaVersion = schemaVersion
        )

        val (_, _, event2Date, _, _) = createEvent(
          status = TransformationRecoverableFailure,
          executionDate = ExecutionDate(timestampsInTheFuture.generateOne),
          projectId = projectId,
          projectPath = projectPath,
          payloadSchemaVersion = schemaVersion
        )

        val (_, _, event3Date, _, _) = createEvent(
          status = TriplesGenerated,
          executionDate = ExecutionDate(timestampsInTheFuture.generateOne),
          projectId = projectId,
          projectPath = projectPath,
          payloadSchemaVersion = schemaVersion
        )

        findEvents(TransformingTriples) shouldBe List.empty

        expectAwaitingTransformationGaugeDecrement(projectPath)
        expectUnderTransformationGaugeIncrement(projectPath)

        givenPrioritisation(
          takes = List(ProjectInfo(projectId, projectPath, List(event1Date, event2Date, event3Date).max, 0)),
          returns = List(ProjectIds(projectId, projectPath) -> MaxPriority)
        )

        finder.popEvent().unsafeRunSync() shouldBe Some(
          TriplesGeneratedEvent(event1Id, projectPath, eventPayload1, schemaVersion)
        )

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
    val schemaVersion = projectSchemaVersions.generateOne

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

  private def createEvent(status:               EventStatus,
                          eventDate:            EventDate = eventDates.generateOne,
                          executionDate:        ExecutionDate = executionDatesInThePast.generateOne,
                          batchDate:            BatchDate = batchDates.generateOne,
                          projectId:            Id = projectIds.generateOne,
                          projectPath:          Path = projectPaths.generateOne,
                          payloadSchemaVersion: SchemaVersion = projectSchemaVersions.generateOne,
                          eventPayload:         EventPayload = eventPayloads.generateOne
  ): (CompoundEventId, EventBody, EventDate, Path, EventPayload) = {
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
      maybeEventPayload = eventPayload.some,
      maybeSchemaVersion = payloadSchemaVersion.some
    )

    (eventId, eventBody, eventDate, projectPath, eventPayload)
  }
}
