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

package io.renku.eventlog.events.categories.creation

import cats.data.Kleisli
import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.eventlog._
import io.renku.eventlog.events.categories.creation.EventPersister.Result._
import io.renku.eventlog.events.categories.creation.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{CompoundEventId, EventBody, EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, TestLabeledHistogram}
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.implicits._

import java.time.Instant
import java.time.temporal.ChronoUnit.HOURS

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
        val newEvent = newEvents.generateOne

        // storeNewEvent 1
        (waitingEventsGauge.increment _).expects(newEvent.project.path).returning(IO.unit)

        persister.storeNewEvent(newEvent).unsafeRunSync() shouldBe Created(newEvent)

        storedEvent(newEvent.compoundEventId) shouldBe (
          newEvent.compoundEventId,
          New,
          CreatedDate(now),
          ExecutionDate(now),
          newEvent.date,
          newEvent.body,
          None
        )
        storedProjects shouldBe List((newEvent.project.id, newEvent.project.path, newEvent.date))

        // storeNewEvent 2 - different event id and different project
        val event2 = newEvents.generateOne
        (waitingEventsGauge.increment _).expects(event2.project.path).returning(IO.unit)

        val nowForEvent2 = Instant.now()
        currentTime.expects().returning(nowForEvent2)

        persister.storeNewEvent(event2).unsafeRunSync() shouldBe Created(event2)

        val save2Event1 +: save2Event2 +: Nil = findEvents(status = New)
        save2Event1 shouldBe (newEvent.compoundEventId, ExecutionDate(now), newEvent.batchDate)
        save2Event2 shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2), event2.batchDate)

        queriesExecTimes.verifyExecutionTimeMeasured("new - check existence",
                                                     "new - find batch",
                                                     "new - create (NEW)",
                                                     "new - upsert project"
        )
      }

    "add a new event if there is no event with the given id for the given project " +
      "and there's a batch for that project waiting and under processing" in new TestCase {
        val newEvent = newEvents.generateOne

        // storeNewEvent 1
        (waitingEventsGauge.increment _).expects(newEvent.project.path).returning(IO.unit)

        persister.storeNewEvent(newEvent).unsafeRunSync() shouldBe a[Created]

        findEvents(status = New).head shouldBe (
          newEvent.compoundEventId, ExecutionDate(now), newEvent.batchDate
        )

        // storeNewEvent 2 - different event id and batch date but same project
        val event2 = newEvent.copy(id = eventIds.generateOne, batchDate = batchDates.generateOne)
        (waitingEventsGauge.increment _).expects(event2.project.path).returning(IO.unit)

        val nowForEvent2 = Instant.now()
        currentTime.expects().returning(nowForEvent2)

        persister.storeNewEvent(event2).unsafeRunSync() shouldBe a[Created]

        val save2Event1 +: save2Event2 +: Nil = findEvents(status = New)
        save2Event1 shouldBe (newEvent.compoundEventId, ExecutionDate(now), newEvent.batchDate)
        save2Event2 shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2), newEvent.batchDate)
      }

    "update latest_event_date for a project " +
      "only if there's an event with more recent Event Date added" in new TestCase {
        val event1 = newEvents.generateOne.copy(date = EventDate(now.minus(2, HOURS)))

        // storing event 1
        (waitingEventsGauge.increment _).expects(event1.project.path).returning(IO.unit)

        persister.storeNewEvent(event1).unsafeRunSync() shouldBe a[Created]

        // storing event 2 for the same project but more recent Event Date
        val event2 = newEvents.generateOne.copy(project = event1.project, date = EventDate(now.minus(1, HOURS)))
        (waitingEventsGauge.increment _).expects(event2.project.path).returning(IO.unit)
        val nowForEvent2 = Instant.now()
        currentTime.expects().returning(nowForEvent2)

        persister.storeNewEvent(event2).unsafeRunSync() shouldBe a[Created]

        // storing event 3 for the same project but less recent Event Date
        val event3 = newEvents.generateOne.copy(project = event1.project, date = EventDate(now.minus(3, HOURS)))
        (waitingEventsGauge.increment _).expects(event3.project.path).returning(IO.unit)
        val nowForEvent3 = Instant.now()
        currentTime.expects().returning(nowForEvent3)

        persister.storeNewEvent(event3).unsafeRunSync() shouldBe a[Created]

        val savedEvent1 +: savedEvent2 +: savedEvent3 +: Nil = findEvents(status = New).noBatchDate
        savedEvent1    shouldBe (event1.compoundEventId, ExecutionDate(now))
        savedEvent2    shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2))
        savedEvent3    shouldBe (event3.compoundEventId, ExecutionDate(nowForEvent3))
        storedProjects shouldBe List((event1.project.id, event1.project.path, event2.date))
      }

    "update latest_event_date and project_path for a project " +
      "only if there's an event with more recent Event Date added" in new TestCase {
        val event1 = newEvents.generateOne.copy(date = EventDate(now.minus(2, HOURS)))

        // storing event 1
        (waitingEventsGauge.increment _).expects(event1.project.path).returning(IO.unit)

        persister.storeNewEvent(event1).unsafeRunSync() shouldBe a[Created]

        // storing event 2 for the same project but with different project_path and more recent Event Date
        val event2 = newEvents.generateOne.copy(project = event1.project.copy(path = projectPaths.generateOne),
                                                date = EventDate(now.minus(1, HOURS))
        )
        (waitingEventsGauge.increment _).expects(event2.project.path).returning(IO.unit)
        val nowForEvent2 = Instant.now()
        currentTime.expects().returning(nowForEvent2)

        persister.storeNewEvent(event2).unsafeRunSync() shouldBe a[Created]

        val savedEvent1 +: savedEvent2 +: Nil = findEvents(status = New).noBatchDate
        savedEvent1    shouldBe (event1.compoundEventId, ExecutionDate(now))
        savedEvent2    shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2))
        storedProjects shouldBe List((event1.project.id, event2.project.path, event2.date))
      }

    "do not update latest_event_date and project_path for a project " +
      "only if there's an event with less recent Event Date added" in new TestCase {
        val event1 = newEvents.generateOne.copy(date = EventDate(now.minus(2, HOURS)))

        // storing event 1
        (waitingEventsGauge.increment _).expects(event1.project.path).returning(IO.unit)

        persister.storeNewEvent(event1).unsafeRunSync() shouldBe a[Created]

        // storing event 2 for the same project but with different project_path and less recent Event Date
        val event2 = newEvents.generateOne.copy(project = event1.project.copy(path = projectPaths.generateOne),
                                                date = EventDate(now.minus(3, HOURS))
        )
        (waitingEventsGauge.increment _).expects(event2.project.path).returning(IO.unit)
        val nowForEvent2 = Instant.now()
        currentTime.expects().returning(nowForEvent2)

        persister.storeNewEvent(event2).unsafeRunSync() shouldBe a[Created]

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

      val nowForEvent2 = Instant.now()
      currentTime.expects().returning(nowForEvent2)

      persister.storeNewEvent(event2).unsafeRunSync() shouldBe a[Created]

      findEvents(status = New) shouldBe Nil

      findEvents(status =
        TriplesStore
      ).eventIdsOnly should contain theSameElementsAs event1.compoundEventId :: event2.compoundEventId :: Nil

    }

    "add a *skipped* event if there is no event with the given id for the given project " in new TestCase {
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

      val nowForEvent2 = Instant.now()
      currentTime.expects().returning(nowForEvent2)

      persister.storeNewEvent(skippedEvent2).unsafeRunSync() shouldBe a[Created]

      val save2Event1 +: save2Event2 +: Nil = findEvents(status = Skipped)
      save2Event1 shouldBe (skippedEvent.compoundEventId, ExecutionDate(now), skippedEvent.batchDate)
      save2Event2 shouldBe (skippedEvent2.compoundEventId, ExecutionDate(nowForEvent2), skippedEvent2.batchDate)

      queriesExecTimes.verifyExecutionTimeMeasured("new - check existence",
                                                   "new - find batch",
                                                   "new - create (SKIPPED)",
                                                   "new - upsert project"
      )
    }

    "add a new event if there is another event with the same id but for a different project" in new TestCase {
      val newEvent = newEvents.generateOne

      // Save 1
      (waitingEventsGauge.increment _).expects(newEvent.project.path).returning(IO.unit)

      persister.storeNewEvent(newEvent).unsafeRunSync() shouldBe a[Created]

      val save1Event1 +: Nil = findEvents(status = New)
      save1Event1 shouldBe (newEvent.compoundEventId, ExecutionDate(now), newEvent.batchDate)

      // Save 2 - the same event id but different project
      val event2 = newEvents.generateOne.copy(id = newEvent.id)
      (waitingEventsGauge.increment _).expects(event2.project.path).returning(IO.unit)

      val nowForEvent2 = Instant.now()
      currentTime.expects().returning(nowForEvent2)

      persister.storeNewEvent(event2).unsafeRunSync() shouldBe a[Created]

      val save2Event1 +: save2Event2 +: Nil = findEvents(status = New)
      save2Event1 shouldBe (newEvent.compoundEventId, ExecutionDate(now), newEvent.batchDate)
      save2Event2 shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2), event2.batchDate)
    }

    "do nothing if there is an event with the same id and project in the DB already" in new TestCase {
      val newEvent = newEvents.generateOne

      (waitingEventsGauge.increment _).expects(newEvent.project.path).returning(IO.unit)

      persister.storeNewEvent(newEvent).unsafeRunSync() shouldBe a[Created]

      storedEvent(newEvent.compoundEventId)._1 shouldBe newEvent.compoundEventId

      persister.storeNewEvent(newEvent.copy(body = eventBodies.generateOne)).unsafeRunSync() shouldBe Existed

      storedEvent(newEvent.compoundEventId) shouldBe (
        newEvent.compoundEventId,
        New,
        CreatedDate(now),
        ExecutionDate(now),
        newEvent.date,
        newEvent.body,
        None
      )
    }
  }

  private trait TestCase {

    val currentTime        = mockFunction[Instant]
    val waitingEventsGauge = mock[LabeledGauge[IO, projects.Path]]
    val queriesExecTimes   = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val persister          = new EventPersisterImpl(sessionResource, waitingEventsGauge, queriesExecTimes, currentTime)

    val now = Instant.now()
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
