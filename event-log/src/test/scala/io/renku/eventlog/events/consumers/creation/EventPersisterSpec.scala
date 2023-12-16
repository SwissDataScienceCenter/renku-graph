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

package io.renku.eventlog.events.consumers.creation

import cats.effect.IO
import cats.syntax.all._
import io.renku.eventlog.events.consumers.creation.EventPersister.Result._
import io.renku.eventlog.events.consumers.creation.Generators._
import io.renku.eventlog.metrics.{EventStatusGauges, QueriesExecutionTimes, TestEventStatusGauges, TestQueriesExecutionTimes}
import TestEventStatusGauges._
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectSlugs
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{OptionValues, Succeeded}

import java.time.Instant
import java.time.temporal.ChronoUnit.{HOURS, MICROS}

class EventPersisterSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers
    with OptionValues {

  "storeNewEvent" should {

    "add a *new* event if there is no event with the given id for the given project " +
      "and there's no batch waiting or under processing" in testDBResource.use { implicit cfg =>
        for {
          // storeNewEvent 1
          event1 <- newEvents.generateOne.pure[IO]
          _      <- persister(now).storeNewEvent(event1).asserting(_ shouldBe Created(event1))

          _ <- gauges.awaitingGeneration.getValue(event1.project.slug).asserting(_ shouldBe 1d)

          _ <- findEvent(event1.compoundEventId).map(_.value).asserting {
                 _ shouldBe FoundEvent(event1.compoundEventId,
                                       ExecutionDate(now),
                                       CreatedDate(now),
                                       event1.date,
                                       New,
                                       event1.body,
                                       event1.batchDate,
                                       None
                 )
               }
          _ <- findProjects.asserting(_ shouldBe List(FoundProject(event1.project, event1.date)))

          // storeNewEvent 2 - different event id and different project
          event2       = newEvents.generateOne
          nowForEvent2 = Instant.now().truncatedTo(MICROS)
          _ <- persister(nowForEvent2).storeNewEvent(event2).asserting(_ shouldBe Created(event2))

          _ <- gauges.awaitingGeneration.getValue(event2.project.slug).asserting(_ shouldBe 1d)

          _ <- findEvents(status = New).asserting {
                 _.map(_.select(Field.Id, Field.ExecutionDate, Field.BatchDate)) shouldBe List(
                   FoundEvent(event1.compoundEventId, ExecutionDate(now), event1.batchDate),
                   FoundEvent(event2.compoundEventId, ExecutionDate(nowForEvent2), event2.batchDate)
                 )
               }
        } yield Succeeded
      }

    "add a new event if there is no event with the given id for the given project " +
      "and there's a batch for that project waiting and under processing" in testDBResource.use { implicit cfg =>
        for {
          // storeNewEvent 1
          event1 <- newEvents.generateOne.pure[IO]
          _      <- persister(now).storeNewEvent(event1).asserting(_ shouldBe a[Created])

          _ <- gauges.awaitingGeneration.getValue(event1.project.slug).asserting(_ shouldBe 1d)

          _ <- findEvents(status = New).asserting(
                 _.head.select(Field.Id, Field.ExecutionDate, Field.BatchDate) shouldBe
                   FoundEvent(event1.compoundEventId, ExecutionDate(now), event1.batchDate)
               )

          // storeNewEvent 2 - different event id and batch date but same project
          event2       = event1.copy(id = eventIds.generateOne, batchDate = batchDates.generateOne)
          nowForEvent2 = Instant.now().truncatedTo(MICROS)
          _ <- persister(nowForEvent2).storeNewEvent(event2).asserting(_ shouldBe a[Created])

          _ <- gauges.awaitingGeneration.getValue(event2.project.slug).asserting(_ shouldBe 2d)

          _ <- findEvents(status = New).asserting {
                 _.map(_.select(Field.Id, Field.ExecutionDate, Field.BatchDate)) shouldBe List(
                   FoundEvent(event1.compoundEventId, ExecutionDate(now), event1.batchDate),
                   FoundEvent(event2.compoundEventId, ExecutionDate(nowForEvent2), event1.batchDate)
                 )
               }
        } yield Succeeded
      }

    "update latest_event_date for a project " +
      "only if there's an event with more recent Event Date added" in testDBResource.use { implicit cfg =>
        for {
          // storing event 1
          event1 <- newEvents.generateOne.copy(date = EventDate(now.minus(2, HOURS))).pure[IO]
          _      <- persister(now).storeNewEvent(event1).asserting(_ shouldBe a[Created])
          _      <- gauges.awaitingGeneration.getValue(event1.project.slug).asserting(_ shouldBe 1d)

          // storing event 2 for the same project but more recent Event Date
          event2       = newEvents.generateOne.copy(project = event1.project, date = EventDate(now.minus(1, HOURS)))
          nowForEvent2 = Instant.now().truncatedTo(MICROS)
          _ <- persister(nowForEvent2).storeNewEvent(event2).asserting(_ shouldBe a[Created])
          _ <- gauges.awaitingGeneration.getValue(event2.project.slug).asserting(_ shouldBe 2d)

          // storing event 3 for the same project but less recent Event Date
          event3       = newEvents.generateOne.copy(project = event1.project, date = EventDate(now.minus(3, HOURS)))
          nowForEvent3 = Instant.now().truncatedTo(MICROS)
          _ <- persister(nowForEvent3).storeNewEvent(event3).asserting(_ shouldBe a[Created])
          _ <- gauges.awaitingGeneration.getValue(event3.project.slug).asserting(_ shouldBe 3d)

          _ <- findEvents(status = New).asserting {
                 _.map(_.select(Field.Id, Field.ExecutionDate)) shouldBe List(
                   FoundEvent(event1.compoundEventId, ExecutionDate(now)),
                   FoundEvent(event2.compoundEventId, ExecutionDate(nowForEvent2)),
                   FoundEvent(event3.compoundEventId, ExecutionDate(nowForEvent3))
                 )
               }
          _ <- findProjects.asserting(_ shouldBe List(FoundProject(event1.project, event2.date)))
        } yield Succeeded
      }

    "update latest_event_date and project_slug for a project " +
      "only if there's an event with more recent Event Date added" in testDBResource.use { implicit cfg =>
        for {
          // storing event 1
          event1 <- newEvents.generateOne.copy(date = EventDate(now.minus(2, HOURS))).pure[IO]
          _      <- persister(now).storeNewEvent(event1).asserting(_ shouldBe a[Created])
          _      <- gauges.awaitingGeneration.getValue(event1.project.slug).asserting(_ shouldBe 1d)

          // storing event 2 for the same project but with different slug and more recent Event Date
          event2 = newEvents.generateOne.copy(project = event1.project.copy(slug = projectSlugs.generateOne),
                                              date = EventDate(now.minus(1, HOURS))
                   )
          nowForEvent2 = Instant.now().truncatedTo(MICROS)
          _ <- persister(nowForEvent2).storeNewEvent(event2).asserting(_ shouldBe a[Created])
          _ <- gauges.awaitingGeneration.getValue(event2.project.slug).asserting(_ shouldBe 1d)

          _ <- findEvents(status = New).asserting {
                 _.map(_.select(Field.Id, Field.ExecutionDate)) shouldBe List(
                   FoundEvent(event1.compoundEventId, ExecutionDate(now)),
                   FoundEvent(event2.compoundEventId, ExecutionDate(nowForEvent2))
                 )
               }
          _ <- findProjects.asserting(
                 _ shouldBe List(FoundProject(Project(event1.project.id, event2.project.slug), event2.date))
               )
        } yield Succeeded
      }

    "do not update latest_event_date and project_slug for a project " +
      "only if there's an event with less recent Event Date added" in testDBResource.use { implicit cfg =>
        for {
          // storing event 1
          event1 <- newEvents.generateOne.copy(date = EventDate(now.minus(2, HOURS))).pure[IO]
          _      <- persister(now).storeNewEvent(event1).asserting(_ shouldBe a[Created])
          _      <- gauges.awaitingGeneration.getValue(event1.project.slug).asserting(_ shouldBe 1d)

          // storing event 2 for the same project but with different slug and less recent Event Date
          event2 = newEvents.generateOne.copy(project = event1.project.copy(slug = projectSlugs.generateOne),
                                              date = EventDate(now.minus(3, HOURS))
                   )
          nowForEvent2 = Instant.now().truncatedTo(MICROS)
          _ <- persister(nowForEvent2).storeNewEvent(event2).asserting(_ shouldBe a[Created])
          _ <- gauges.awaitingGeneration.getValue(event2.project.slug).asserting(_ shouldBe 1d)

          _ <- findEvents(status = New).asserting {
                 _.map(_.select(Field.Id, Field.ExecutionDate)) shouldBe List(
                   FoundEvent(event1.compoundEventId, ExecutionDate(now)),
                   FoundEvent(event2.compoundEventId, ExecutionDate(nowForEvent2))
                 )
               }
          _ <- findProjects.asserting(_ shouldBe List(FoundProject(event1.project, event1.date)))
        } yield Succeeded
      }

    "create event with the TRIPLES_STORE status if a newer event has the TRIPLES_STORE status already" in testDBResource
      .use { implicit cfg =>
        for {
          // storing event 1
          event1 <- newEvents.generateOne.copy(date = EventDate(now.minus(2, HOURS)), status = TriplesStore).pure[IO]
          _      <- persister(now).storeNewEvent(event1).asserting(_ shouldBe a[Created])

          // storing event 2 older than event1
          event2       = newEvents.generateOne.copy(project = event1.project, date = EventDate(now.minus(3, HOURS)))
          nowForEvent2 = Instant.now().truncatedTo(MICROS)
          _ <- persister(nowForEvent2).storeNewEvent(event2).asserting(_ shouldBe a[Created])

          _ <- findEvents(status = New).asserting(_ shouldBe Nil)
          _ <- findEvents(status = TriplesStore).asserting(
                 _.map(_.id) should contain theSameElementsAs (event1.compoundEventId :: event2.compoundEventId :: Nil)
               )
        } yield Succeeded
      }

    "add a *SKIPPED* event if there is no event with the given id for the given project " in testDBResource.use {
      implicit cfg =>
        val skippedEvent = skippedEvents.generateOne

        for {
          // storeNewEvent 1
          _ <- persister(now).storeNewEvent(skippedEvent).asserting(_ shouldBe a[Created])

          _ <- findEvent(skippedEvent.compoundEventId).map(_.value).asserting {
                 _ shouldBe FoundEvent(
                   skippedEvent.compoundEventId,
                   ExecutionDate(now),
                   CreatedDate(now),
                   skippedEvent.date,
                   Skipped,
                   skippedEvent.body,
                   skippedEvent.batchDate,
                   Some(skippedEvent.message)
                 )
               }
          _ <- findProjects.asserting(_ shouldBe List(FoundProject(skippedEvent.project, skippedEvent.date)))

          // storeNewEvent 2 - different event id and different project
          skippedEvent2 = skippedEvents.generateOne
          nowForEvent2  = Instant.now().truncatedTo(MICROS)
          _ <- persister(nowForEvent2).storeNewEvent(skippedEvent2).asserting(_ shouldBe a[Created])

          _ <- findEvents(status = Skipped).asserting {
                 _.map(_.select(Field.Id, Field.ExecutionDate, Field.BatchDate)) shouldBe List(
                   FoundEvent(skippedEvent.compoundEventId, ExecutionDate(now), skippedEvent.batchDate),
                   FoundEvent(skippedEvent2.compoundEventId, ExecutionDate(nowForEvent2), skippedEvent2.batchDate)
                 )
               }
        } yield Succeeded
    }

    "add a new event if there is another event with the same id but for a different project" in testDBResource.use {
      implicit cfg =>
        for {
          // Save 1
          event1 <- newEvents.generateOne.pure[IO]
          _      <- persister(now).storeNewEvent(event1).asserting(_ shouldBe a[Created])
          _      <- gauges.awaitingGeneration.getValue(event1.project.slug).asserting(_ shouldBe 1d)

          _ <- findEvents(status = New).asserting {
                 _.map(_.select(Field.Id, Field.ExecutionDate, Field.BatchDate)) shouldBe List(
                   FoundEvent(event1.compoundEventId, ExecutionDate(now), event1.batchDate)
                 )
               }

          // Save 2 - the same event id but different project
          event2       = newEvents.generateOne.copy(id = event1.id)
          nowForEvent2 = Instant.now().truncatedTo(MICROS)
          _ <- persister(nowForEvent2).storeNewEvent(event2).asserting(_ shouldBe a[Created])
          _ <- gauges.awaitingGeneration.getValue(event2.project.slug).asserting(_ shouldBe 1d)

          _ <- findEvents(status = New).asserting {
                 _.map(_.select(Field.Id, Field.ExecutionDate, Field.BatchDate)) shouldBe List(
                   FoundEvent(event1.compoundEventId, ExecutionDate(now), event1.batchDate),
                   FoundEvent(event2.compoundEventId, ExecutionDate(nowForEvent2), event2.batchDate)
                 )
               }
        } yield Succeeded
    }

    "do nothing if there is an event with the same id and project in the DB already" in testDBResource.use {
      implicit cfg =>
        for {
          event <- newEvents.generateOne.pure[IO]
          _     <- persister(now).storeNewEvent(event).asserting(_ shouldBe a[Created])
          _     <- findEvent(event.compoundEventId).asserting(_.map(_.id) shouldBe event.compoundEventId.some)

          _ <- persister(now).storeNewEvent(event.copy(body = eventBodies.generateOne)).asserting(_ shouldBe Existed)
          _ <- gauges.awaitingGeneration.getValue(event.project.slug).asserting(_ shouldBe 1d)

          _ <- findEvent(event.compoundEventId).map(_.value).asserting {
                 _ shouldBe FoundEvent(event.compoundEventId,
                                       ExecutionDate(now),
                                       CreatedDate(now),
                                       event.date,
                                       New,
                                       event.body,
                                       event.batchDate,
                                       None
                 )
               }
        } yield Succeeded
    }
  }

  private implicit lazy val gauges: EventStatusGauges[IO] = TestEventStatusGauges[IO]
  private lazy val now = Instant.now().truncatedTo(MICROS)
  private def persister(currentTime: Instant = now)(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val queriesExecTimes: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new EventPersisterImpl[IO](() => currentTime)
  }
}
