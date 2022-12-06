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

package io.renku.eventlog.events.consumers.creation

import cats.data.Kleisli
import cats.effect.IO
import io.renku.eventlog.events.consumers.creation.EventPersister.Result._
import io.renku.eventlog.events.consumers.creation.Generators._
import io.renku.eventlog.metrics.{EventStatusGauges, QueriesExecutionTimes, TestEventStatusGauges}
import TestEventStatusGauges._
import io.renku.eventlog.{InMemoryEventLogDbSpec, TypeSerializers}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import io.renku.graph.model.projects
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.implicits._

import java.time.Instant
import java.time.temporal.ChronoUnit.{HOURS, MICROS}

class EventPersisterSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with TypeSerializers
    with should.Matchers {

  "storeNewEvent" should {

    "add a *new* event if there is no event with the given id for the given project " +
      "and there's no batch waiting or under processing" in new TestCase {

        // storeNewEvent 1
        val event1 = newEvents.generateOne

        persister.storeNewEvent(event1).unsafeRunSync() shouldBe Created(event1)

        gauges.awaitingGeneration.getValue(event1.project.path).unsafeRunSync() shouldBe 1d

        storedEvent(event1.compoundEventId) shouldBe (
          event1.compoundEventId,
          New,
          CreatedDate(now),
          ExecutionDate(now),
          event1.date,
          event1.body,
          None
        )
        storedProjects shouldBe List((event1.project.id, event1.project.path, event1.date))

        // storeNewEvent 2 - different event id and different project
        val event2 = newEvents.generateOne

        val nowForEvent2 = Instant.now().truncatedTo(MICROS)
        currentTime.expects().returning(nowForEvent2)

        persister.storeNewEvent(event2).unsafeRunSync() shouldBe Created(event2)

        gauges.awaitingGeneration.getValue(event2.project.path).unsafeRunSync() shouldBe 1d

        val save2Event1 +: save2Event2 +: Nil = findEvents(status = New)
        save2Event1 shouldBe (event1.compoundEventId, ExecutionDate(now), event1.batchDate)
        save2Event2 shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2), event2.batchDate)
      }

    "add a new event if there is no event with the given id for the given project " +
      "and there's a batch for that project waiting and under processing" in new TestCase {

        // storeNewEvent 1
        val event1 = newEvents.generateOne

        persister.storeNewEvent(event1).unsafeRunSync() shouldBe a[Created]

        gauges.awaitingGeneration.getValue(event1.project.path).unsafeRunSync() shouldBe 1d

        findEvents(status = New).head shouldBe (
          event1.compoundEventId, ExecutionDate(now), event1.batchDate
        )

        // storeNewEvent 2 - different event id and batch date but same project
        val event2 = event1.copy(id = eventIds.generateOne, batchDate = batchDates.generateOne)

        val nowForEvent2 = Instant.now().truncatedTo(MICROS)
        currentTime.expects().returning(nowForEvent2)

        persister.storeNewEvent(event2).unsafeRunSync() shouldBe a[Created]

        gauges.awaitingGeneration.getValue(event2.project.path).unsafeRunSync() shouldBe 2d

        val save2Event1 +: save2Event2 +: Nil = findEvents(status = New)
        save2Event1 shouldBe (event1.compoundEventId, ExecutionDate(now), event1.batchDate)
        save2Event2 shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2), event1.batchDate)
      }

    "update latest_event_date for a project " +
      "only if there's an event with more recent Event Date added" in new TestCase {

        // storing event 1
        val event1 = newEvents.generateOne.copy(date = EventDate(now.minus(2, HOURS)))

        persister.storeNewEvent(event1).unsafeRunSync() shouldBe a[Created]

        gauges.awaitingGeneration.getValue(event1.project.path).unsafeRunSync() shouldBe 1d

        // storing event 2 for the same project but more recent Event Date
        val event2       = newEvents.generateOne.copy(project = event1.project, date = EventDate(now.minus(1, HOURS)))
        val nowForEvent2 = Instant.now().truncatedTo(MICROS)
        currentTime.expects().returning(nowForEvent2)

        persister.storeNewEvent(event2).unsafeRunSync() shouldBe a[Created]

        gauges.awaitingGeneration.getValue(event2.project.path).unsafeRunSync() shouldBe 2d

        // storing event 3 for the same project but less recent Event Date
        val event3       = newEvents.generateOne.copy(project = event1.project, date = EventDate(now.minus(3, HOURS)))
        val nowForEvent3 = Instant.now().truncatedTo(MICROS)
        currentTime.expects().returning(nowForEvent3)

        persister.storeNewEvent(event3).unsafeRunSync() shouldBe a[Created]

        gauges.awaitingGeneration.getValue(event3.project.path).unsafeRunSync() shouldBe 3d

        val savedEvent1 +: savedEvent2 +: savedEvent3 +: Nil = findEvents(status = New).noBatchDate
        savedEvent1    shouldBe (event1.compoundEventId, ExecutionDate(now))
        savedEvent2    shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2))
        savedEvent3    shouldBe (event3.compoundEventId, ExecutionDate(nowForEvent3))
        storedProjects shouldBe List((event1.project.id, event1.project.path, event2.date))
      }

    "update latest_event_date and project_path for a project " +
      "only if there's an event with more recent Event Date added" in new TestCase {

        // storing event 1
        val event1 = newEvents.generateOne.copy(date = EventDate(now.minus(2, HOURS)))

        persister.storeNewEvent(event1).unsafeRunSync() shouldBe a[Created]

        gauges.awaitingGeneration.getValue(event1.project.path).unsafeRunSync() shouldBe 1d

        // storing event 2 for the same project but with different project_path and more recent Event Date
        val event2 = newEvents.generateOne.copy(project = event1.project.copy(path = projectPaths.generateOne),
                                                date = EventDate(now.minus(1, HOURS))
        )
        val nowForEvent2 = Instant.now().truncatedTo(MICROS)
        currentTime.expects().returning(nowForEvent2)

        persister.storeNewEvent(event2).unsafeRunSync() shouldBe a[Created]

        gauges.awaitingGeneration.getValue(event2.project.path).unsafeRunSync() shouldBe 1d

        val savedEvent1 +: savedEvent2 +: Nil = findEvents(status = New).noBatchDate
        savedEvent1    shouldBe (event1.compoundEventId, ExecutionDate(now))
        savedEvent2    shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2))
        storedProjects shouldBe List((event1.project.id, event2.project.path, event2.date))
      }

    "do not update latest_event_date and project_path for a project " +
      "only if there's an event with less recent Event Date added" in new TestCase {

        // storing event 1
        val event1 = newEvents.generateOne.copy(date = EventDate(now.minus(2, HOURS)))

        persister.storeNewEvent(event1).unsafeRunSync() shouldBe a[Created]

        gauges.awaitingGeneration.getValue(event1.project.path).unsafeRunSync() shouldBe 1d

        // storing event 2 for the same project but with different project_path and less recent Event Date
        val event2 = newEvents.generateOne.copy(project = event1.project.copy(path = projectPaths.generateOne),
                                                date = EventDate(now.minus(3, HOURS))
        )
        val nowForEvent2 = Instant.now().truncatedTo(MICROS)
        currentTime.expects().returning(nowForEvent2)

        persister.storeNewEvent(event2).unsafeRunSync() shouldBe a[Created]

        gauges.awaitingGeneration.getValue(event2.project.path).unsafeRunSync() shouldBe 1d

        val savedEvent1 +: savedEvent2 +: Nil = findEvents(status = New).noBatchDate
        savedEvent1    shouldBe (event1.compoundEventId, ExecutionDate(now))
        savedEvent2    shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2))
        storedProjects shouldBe List((event1.project.id, event1.project.path, event1.date))
      }

    "create event with the TRIPLES_STORE status if a newer event has the TRIPLES_STORE status already" in new TestCase {
      val event1 = newEvents.generateOne.copy(date = EventDate(now.minus(2, HOURS)), status = TriplesStore)

      // storing event 1
      persister.storeNewEvent(event1).unsafeRunSync() shouldBe a[Created]

      // storing event 2 older than event1
      val event2 = newEvents.generateOne.copy(project = event1.project, date = EventDate(now.minus(3, HOURS)))

      val nowForEvent2 = Instant.now().truncatedTo(MICROS)
      currentTime.expects().returning(nowForEvent2)

      persister.storeNewEvent(event2).unsafeRunSync() shouldBe a[Created]

      findEvents(status = New) shouldBe Nil

      findEvents(status =
        TriplesStore
      ).eventIdsOnly should contain theSameElementsAs event1.compoundEventId :: event2.compoundEventId :: Nil
    }

    "add a *SKIPPED* event if there is no event with the given id for the given project " in new TestCase {
      val skippedEvent = skippedEvents.generateOne

      // storeNewEvent 1
      persister.storeNewEvent(skippedEvent).unsafeRunSync() shouldBe a[Created]

      storedEvent(skippedEvent.compoundEventId) shouldBe (
        skippedEvent.compoundEventId,
        Skipped,
        CreatedDate(now),
        ExecutionDate(now),
        skippedEvent.date,
        skippedEvent.body,
        Some(skippedEvent.message)
      )
      storedProjects shouldBe List((skippedEvent.project.id, skippedEvent.project.path, skippedEvent.date))

      // storeNewEvent 2 - different event id and different project
      val skippedEvent2 = skippedEvents.generateOne

      val nowForEvent2 = Instant.now().truncatedTo(MICROS)
      currentTime.expects().returning(nowForEvent2)

      persister.storeNewEvent(skippedEvent2).unsafeRunSync() shouldBe a[Created]

      val save2Event1 +: save2Event2 +: Nil = findEvents(status = Skipped)
      save2Event1 shouldBe (skippedEvent.compoundEventId, ExecutionDate(now), skippedEvent.batchDate)
      save2Event2 shouldBe (skippedEvent2.compoundEventId, ExecutionDate(nowForEvent2), skippedEvent2.batchDate)
    }

    "add a new event if there is another event with the same id but for a different project" in new TestCase {

      // Save 1
      val event1 = newEvents.generateOne

      persister.storeNewEvent(event1).unsafeRunSync() shouldBe a[Created]

      gauges.awaitingGeneration.getValue(event1.project.path).unsafeRunSync() shouldBe 1d

      val save1Event1 +: Nil = findEvents(status = New)
      save1Event1 shouldBe (event1.compoundEventId, ExecutionDate(now), event1.batchDate)

      // Save 2 - the same event id but different project
      val event2 = newEvents.generateOne.copy(id = event1.id)

      val nowForEvent2 = Instant.now().truncatedTo(MICROS)
      currentTime.expects().returning(nowForEvent2)

      persister.storeNewEvent(event2).unsafeRunSync() shouldBe a[Created]

      gauges.awaitingGeneration.getValue(event2.project.path).unsafeRunSync() shouldBe 1d

      val save2Event1 +: save2Event2 +: Nil = findEvents(status = New)
      save2Event1 shouldBe (event1.compoundEventId, ExecutionDate(now), event1.batchDate)
      save2Event2 shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2), event2.batchDate)
    }

    "do nothing if there is an event with the same id and project in the DB already" in new TestCase {

      val event = newEvents.generateOne

      persister.storeNewEvent(event).unsafeRunSync() shouldBe a[Created]

      storedEvent(event.compoundEventId)._1 shouldBe event.compoundEventId

      persister.storeNewEvent(event.copy(body = eventBodies.generateOne)).unsafeRunSync() shouldBe Existed

      gauges.awaitingGeneration.getValue(event.project.path).unsafeRunSync() shouldBe 1d

      storedEvent(event.compoundEventId) shouldBe (
        event.compoundEventId,
        New,
        CreatedDate(now),
        ExecutionDate(now),
        event.date,
        event.body,
        None
      )
    }
  }

  private trait TestCase {

    implicit val gauges:                   EventStatusGauges[IO]     = TestEventStatusGauges[IO]
    private implicit val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    val currentTime = mockFunction[Instant]
    val persister   = new EventPersisterImpl[IO](currentTime)

    val now = Instant.now().truncatedTo(MICROS)
    currentTime.expects().returning(now)

    def storedEvent(
        compoundEventId: CompoundEventId
    ): (CompoundEventId, EventStatus, CreatedDate, ExecutionDate, EventDate, EventBody, Option[EventMessage]) =
      execute {
        Kleisli { session =>
          val query: Query[
            EventId ~ projects.Id,
            (CompoundEventId, EventStatus, CreatedDate, ExecutionDate, EventDate, EventBody, Option[EventMessage])
          ] = sql"""SELECT event_id, project_id, status, created_date, execution_date, event_date, event_body, message
                  FROM event  
                  WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
                  """
            .query(
              eventIdDecoder ~ projectIdDecoder ~ eventStatusDecoder ~ createdDateDecoder ~ executionDateDecoder ~ eventDateDecoder ~ eventBodyDecoder ~ eventMessageDecoder.opt
            )
            .map {
              case eventId ~ projectId ~ eventStatus ~ createdDate ~ executionDate ~ eventDate ~ eventBody ~ maybeEventMessage =>
                (
                  CompoundEventId(eventId, projectId),
                  eventStatus,
                  createdDate,
                  executionDate,
                  eventDate,
                  eventBody,
                  maybeEventMessage
                )
            }
          session.prepare(query).use(_.unique(compoundEventId.id ~ compoundEventId.projectId))
        }
      }
  }

  private def storedProjects: List[(projects.Id, projects.Path, EventDate)] = execute {
    Kleisli { session =>
      val query: Query[Void, (projects.Id, projects.Path, EventDate)] =
        sql"""SELECT project_id, project_path, latest_event_date
              FROM project"""
          .query(projectIdDecoder ~ projectPathDecoder ~ eventDateDecoder)
          .map { case projectId ~ projectPath ~ eventDate => (projectId, projectPath, eventDate) }
      session.execute(query)
    }
  }
}
