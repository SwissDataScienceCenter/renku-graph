/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.TestEventStatusGauges._
import io.renku.eventlog.metrics.{EventStatusGauges, QueriesExecutionTimes, TestEventStatusGauges, TestQueriesExecutionTimes}
import io.renku.eventlog.{CleanUpEventsProvisioning, EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators._
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, Deleting}
import io.renku.graph.model.events._
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import java.time.{Instant, OffsetDateTime}

private class EventFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with CleanUpEventsProvisioning
    with should.Matchers {

  "popEvent" should {

    "return events from the clean_up_events_queue " +
      "and mark AwaitingDeletion events in the events table as Deleted " +
      "when an event for a project having such events is returned" in testDBResource.use { implicit cfg =>
        val project1 = consumerProjects.generateOne
        for {
          _ <- insertCleanUpEvent(project1, date = OffsetDateTime.now().minusSeconds(2))

          project2 = consumerProjects.generateOne
          _ <- insertCleanUpEvent(project2, date = OffsetDateTime.now())
          _ <- upsertProject(project2, EventDate(Instant.EPOCH))

          _ <- generateAndStoreEvent(
                 project1,
                 Gen.oneOf(EventStatus.all - AwaitingDeletion - Deleting).generateOne,
                 ExecutionDate(now minusSeconds 2)
               )
          project1Event2 <- generateAndStoreEvent(project1, AwaitingDeletion, ExecutionDate(now minusSeconds 1))

          _ <- finder.popEvent().asserting(_ shouldBe CleanUpEvent(project1).some)

          _ <- gauges.awaitingDeletion.getValue(project1.slug).asserting(_ shouldBe -1d)
          _ <- gauges.underDeletion.getValue(project1.slug).asserting(_ shouldBe 1d)

          _ <- finder.popEvent().asserting(_ shouldBe CleanUpEvent(project2).some)

          _ <- gauges.awaitingDeletion.getValue(project2.slug).asserting(_ shouldBe 0d)
          _ <- gauges.underDeletion.getValue(project2.slug).asserting(_ shouldBe 0d)

          _ <- finder.popEvent().asserting(_ shouldBe None)

          _ <- findCleanUpEvents.asserting(_ shouldBe Nil)
          _ <- findEvents(Deleting).asserting(_.map(_.id) shouldBe List(project1Event2.eventId))
        } yield Succeeded
      }

    "return events from the clean_up_events_queue first " +
      "and then events for projects having AwaitingDeletion events" in testDBResource.use { implicit cfg =>
        val project1 = consumerProjects.generateOne
        for {
          _ <- insertCleanUpEvent(project1, date = OffsetDateTime.now())
          _ <- upsertProject(project1, EventDate(Instant.EPOCH))

          project2 = consumerProjects.generateOne
          project2Event <- generateAndStoreEvent(project2, AwaitingDeletion, ExecutionDate(now))

          _ <- finder.popEvent().asserting(_ shouldBe CleanUpEvent(project1).some)

          _ <- gauges.awaitingDeletion.getValue(project1.slug).asserting(_ shouldBe 0d)
          _ <- gauges.underDeletion.getValue(project1.slug).asserting(_ shouldBe 0d)

          _ <- finder.popEvent().asserting(_ shouldBe CleanUpEvent(project2).some)

          _ <- gauges.awaitingDeletion.getValue(project2.slug).asserting(_ shouldBe -1d)
          _ <- gauges.underDeletion.getValue(project2.slug).asserting(_ shouldBe 1d)

          _ <- finder.popEvent().asserting(_ shouldBe None)

          _ <- findCleanUpEvents.asserting(_ shouldBe Nil)
          _ <- findEvents(Deleting).asserting(_.map(_.id) shouldBe List(project2Event.eventId))
        } yield Succeeded
      }

    "keep returning events as long as there are projects with AwaitingDeletion events (one event per project) " +
      "starting with projects having events with the oldest Execution Date " +
      "- case with no events in the clean_up_events_queue" in testDBResource.use { implicit cfg =>
        val project1 = consumerProjects.generateOne
        for {
          _ <- generateAndStoreEvent(
                 project1,
                 Gen.oneOf(EventStatus.all - AwaitingDeletion - Deleting).generateOne,
                 ExecutionDate(now minusSeconds 3)
               )
          project1Event2 <- generateAndStoreEvent(project1, AwaitingDeletion, ExecutionDate(now minusSeconds 2))
          project1Event3 <- generateAndStoreEvent(project1, AwaitingDeletion, ExecutionDate(now minusSeconds 1))

          project2 = consumerProjects.generateOne
          project2Event <- generateAndStoreEvent(project2, AwaitingDeletion, ExecutionDate(now))

          _ <- finder.popEvent().asserting(_ shouldBe CleanUpEvent(project1).some)

          _ <- gauges.awaitingDeletion.getValue(project1.slug).asserting(_ shouldBe -2d)
          _ <- gauges.underDeletion.getValue(project1.slug).asserting(_ shouldBe 2d)

          _ <- finder.popEvent().asserting(_ shouldBe CleanUpEvent(project2).some)

          _ <- gauges.awaitingDeletion.getValue(project2.slug).asserting(_ shouldBe -1d)
          _ <- gauges.underDeletion.getValue(project2.slug).asserting(_ shouldBe 1d)

          _ <- finder.popEvent().asserting(_ shouldBe None)

          _ <- findEvents(Deleting).asserting(
                 _.map(_.id) should contain theSameElementsAs
                   List(project1Event2, project1Event3, project2Event).map(_.eventId)
               )
        } yield Succeeded
      }
  }

  private lazy val now = Instant.now()
  private implicit val qet:         QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
  private implicit lazy val gauges: EventStatusGauges[IO]     = TestEventStatusGauges[IO]
  private def finder(implicit cfg: DBConfig[EventLogDB]) = new EventFinderImpl[IO](() => now)

  private def generateAndStoreEvent(project: Project, eventStatus: EventStatus, executionDate: ExecutionDate)(implicit
      cfg: DBConfig[EventLogDB]
  ) = storeGeneratedEvent(eventStatus, eventDates.generateOne, project, executionDate = executionDate)
}
