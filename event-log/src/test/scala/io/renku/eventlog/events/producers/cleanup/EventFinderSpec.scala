/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.producers.cleanup

import cats.effect.IO
import cats.syntax.all._
import io.renku.eventlog.metrics.TestEventStatusGauges._
import io.renku.eventlog.metrics.{EventStatusGauges, QueriesExecutionTimes, TestEventStatusGauges}
import io.renku.eventlog.{CleanUpEventsProvisioning, InMemoryEventLogDbSpec}
import io.renku.events.consumers.ConsumersModelGenerators._
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, Deleting}
import io.renku.graph.model.events._
import io.renku.interpreters.TestLogger
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Instant, OffsetDateTime}

private class EventFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with CleanUpEventsProvisioning
    with MockFactory
    with should.Matchers {

  "popEvent" should {

    "return events from the clean_up_events_queue " +
      "and mark AwaitingDeletion events in the events table as Deleted " +
      "when an event for a project having such events is returned" in new TestCase {

        val project1 = consumerProjects.generateOne
        insertCleanUpEvent(project1, date = OffsetDateTime.now().minusSeconds(2))

        val project2 = consumerProjects.generateOne
        insertCleanUpEvent(project2, date = OffsetDateTime.now())
        upsertProject(project2, EventDate(Instant.EPOCH))

        generateEvent(
          project1,
          Gen.oneOf(EventStatus.all - AwaitingDeletion - Deleting).generateOne,
          ExecutionDate(now minusSeconds 2)
        )
        val project1Event2 = generateEvent(project1, AwaitingDeletion, ExecutionDate(now minusSeconds 1))

        finder.popEvent().unsafeRunSync() shouldBe CleanUpEvent(project1).some

        gauges.awaitingDeletion.getValue(project1.path).unsafeRunSync() shouldBe -1d
        gauges.underDeletion.getValue(project1.path).unsafeRunSync()    shouldBe 1d

        finder.popEvent().unsafeRunSync() shouldBe CleanUpEvent(project2).some

        gauges.awaitingDeletion.getValue(project2.path).unsafeRunSync() shouldBe -1d
        gauges.underDeletion.getValue(project2.path).unsafeRunSync()    shouldBe 1d

        finder.popEvent().unsafeRunSync() shouldBe None

        findCleanUpEvents              shouldBe Nil
        findEvents(Deleting).map(_._1) shouldBe List(project1Event2)
      }

    "return events from the clean_up_events_queue first " +
      "and then events for projects having AwaitingDeletion events" in new TestCase {

        val project1 = consumerProjects.generateOne
        insertCleanUpEvent(project1, date = OffsetDateTime.now())
        upsertProject(project1, EventDate(Instant.EPOCH))

        val project2      = consumerProjects.generateOne
        val project2Event = generateEvent(project2, AwaitingDeletion, ExecutionDate(now))

        finder.popEvent().unsafeRunSync() shouldBe CleanUpEvent(project1).some

        gauges.awaitingDeletion.getValue(project1.path).unsafeRunSync() shouldBe 0d
        gauges.underDeletion.getValue(project1.path).unsafeRunSync()    shouldBe 0d

        finder.popEvent().unsafeRunSync() shouldBe CleanUpEvent(project2).some

        gauges.awaitingDeletion.getValue(project2.path).unsafeRunSync() shouldBe -1d
        gauges.underDeletion.getValue(project2.path).unsafeRunSync()    shouldBe 1d

        finder.popEvent().unsafeRunSync() shouldBe None

        findCleanUpEvents              shouldBe Nil
        findEvents(Deleting).map(_._1) shouldBe List(project2Event)
      }

    "keep returning events as long as there are projects with AwaitingDeletion events (one event per project) " +
      "starting with projects having events with the oldest Execution Date " +
      "- case with no events in the clean_up_events_queue" in new TestCase {

        val project1 = consumerProjects.generateOne
        generateEvent(
          project1,
          Gen.oneOf(EventStatus.all - AwaitingDeletion - Deleting).generateOne,
          ExecutionDate(now minusSeconds 3)
        )
        val project1Event2 = generateEvent(project1, AwaitingDeletion, ExecutionDate(now minusSeconds 2))
        val project1Event3 = generateEvent(project1, AwaitingDeletion, ExecutionDate(now minusSeconds 1))

        val project2      = consumerProjects.generateOne
        val project2Event = generateEvent(project2, AwaitingDeletion, ExecutionDate(now))

        finder.popEvent().unsafeRunSync() shouldBe CleanUpEvent(project1).some

        gauges.awaitingDeletion.getValue(project1.path).unsafeRunSync() shouldBe -2d
        gauges.underDeletion.getValue(project1.path).unsafeRunSync()    shouldBe 2d

        finder.popEvent().unsafeRunSync() shouldBe CleanUpEvent(project2).some

        gauges.awaitingDeletion.getValue(project2.path).unsafeRunSync() shouldBe -1d
        gauges.underDeletion.getValue(project2.path).unsafeRunSync()    shouldBe 1d

        finder.popEvent().unsafeRunSync() shouldBe None

        findEvents(Deleting).map(_._1) should contain theSameElementsAs List(project1Event2,
                                                                             project1Event3,
                                                                             project2Event
        )
      }
  }

  private trait TestCase {
    val now = Instant.now()

    private implicit val logger:           TestLogger[IO]            = TestLogger[IO]()
    private implicit val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    implicit val gauges:                   EventStatusGauges[IO]     = TestEventStatusGauges[IO]
    val currentTime = mockFunction[Instant]
    currentTime.expects().returning(now).anyNumberOfTimes()
    val finder = new EventFinderImpl[IO](currentTime)
  }

  private def generateEvent(project:       Project,
                            eventStatus:   EventStatus,
                            executionDate: ExecutionDate
  ): CompoundEventId = {
    val eventId = compoundEventIds.generateOne.copy(projectId = project.id)
    storeEvent(eventId,
               eventStatus,
               executionDate,
               eventDates.generateOne,
               eventBodies.generateOne,
               projectPath = project.path
    )
    eventId
  }
}
