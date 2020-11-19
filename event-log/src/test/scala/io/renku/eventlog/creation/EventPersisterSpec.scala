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

package io.renku.eventlog.creation

import java.time.Instant

import cats.effect.IO
import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, TestLabeledHistogram}
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.DbEventLogGenerators._
import io.renku.eventlog.Event.SkippedEvent
import io.renku.eventlog.EventStatus.{New, Skipped}
import io.renku.eventlog._
import io.renku.eventlog.creation.EventPersister.Result
import io.renku.eventlog.creation.EventPersister.Result._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventPersisterSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with TypesSerializers
    with should.Matchers {

  "storeNewEvent" should {

    "add a *new* event if there is no event with the given id for the given project " +
      "and there's no batch waiting or under processing" in new TestCase {
        val newEvent = newEvents.generateOne

        // storeNewEvent 1
        (waitingEventsGauge.increment _).expects(newEvent.project.path).returning(IO.unit)

        persister.storeNewEvent(newEvent).unsafeRunSync() shouldBe Created

        storedEvent(newEvent.compoundEventId) shouldBe (
          newEvent.compoundEventId,
          EventStatus.New,
          CreatedDate(now),
          ExecutionDate(now),
          newEvent.date,
          newEvent.body,
          None
        )

        // storeNewEvent 2 - different event id and different project
        val event2 = newEvents.generateOne
        (waitingEventsGauge.increment _).expects(event2.project.path).returning(IO.unit)

        val nowForEvent2 = Instant.now()
        currentTime.expects().returning(nowForEvent2)

        persister.storeNewEvent(event2).unsafeRunSync() shouldBe Created

        val save2Event1 +: save2Event2 +: Nil = findEvents(status = New)
        save2Event1 shouldBe (newEvent.compoundEventId, ExecutionDate(now), newEvent.batchDate)
        save2Event2 shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2), event2.batchDate)

        queriesExecTimes.verifyExecutionTimeMeasured("new - check existence", "new - find batch", "new - create (NEW)")
      }

    "add a new event if there is no event with the given id for the given project " +
      "and there's a batch for that project waiting and under processing" in new TestCase {
        val newEvent = newEvents.generateOne

        // storeNewEvent 1
        (waitingEventsGauge.increment _).expects(newEvent.project.path).returning(IO.unit)

        persister.storeNewEvent(newEvent).unsafeRunSync() shouldBe Created

        findEvents(status = New).head shouldBe (
          newEvent.compoundEventId, ExecutionDate(now), newEvent.batchDate
        )

        // storeNewEvent 2 - different event id and batch date but same project
        val event2 = newEvent.copy(id = eventIds.generateOne, batchDate = batchDates.generateOne)
        (waitingEventsGauge.increment _).expects(event2.project.path).returning(IO.unit)

        val nowForEvent2 = Instant.now()
        currentTime.expects().returning(nowForEvent2)

        persister.storeNewEvent(event2).unsafeRunSync() shouldBe Created

        val save2Event1 +: save2Event2 +: Nil = findEvents(status = New)
        save2Event1 shouldBe (newEvent.compoundEventId, ExecutionDate(now), newEvent.batchDate)
        save2Event2 shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2), newEvent.batchDate)
      }

    "add a *skipped* event if there is no event with the given id for the given project " in new TestCase {
      val skippedEvent = skippedEvents.generateOne

      // storeNewEvent 1
      persister.storeNewEvent(skippedEvent).unsafeRunSync() shouldBe Created

      storedEvent(skippedEvent.compoundEventId) shouldBe (
        skippedEvent.compoundEventId,
        EventStatus.Skipped,
        CreatedDate(now),
        ExecutionDate(now),
        skippedEvent.date,
        skippedEvent.body,
        Some(skippedEvent.message)
      )

      // storeNewEvent 2 - different event id and different project
      val skippedEvent2 = skippedEvents.generateOne

      val nowForEvent2 = Instant.now()
      currentTime.expects().returning(nowForEvent2)

      persister.storeNewEvent(skippedEvent2).unsafeRunSync() shouldBe Created

      val save2Event1 +: save2Event2 +: Nil = findEvents(status = Skipped)
      save2Event1 shouldBe (skippedEvent.compoundEventId, ExecutionDate(now), skippedEvent.batchDate)
      save2Event2 shouldBe (skippedEvent2.compoundEventId, ExecutionDate(nowForEvent2), skippedEvent2.batchDate)

      queriesExecTimes.verifyExecutionTimeMeasured("new - check existence",
                                                   "new - find batch",
                                                   "new - create (SKIPPED)"
      )
    }

    "add a new event if there is another event with the same id but for a different project" in new TestCase {
      val newEvent = newEvents.generateOne

      // Save 1
      (waitingEventsGauge.increment _).expects(newEvent.project.path).returning(IO.unit)

      persister.storeNewEvent(newEvent).unsafeRunSync() shouldBe Created

      val save1Event1 +: Nil = findEvents(status = New)
      save1Event1 shouldBe (newEvent.compoundEventId, ExecutionDate(now), newEvent.batchDate)

      // Save 2 - the same event id but different project
      val event2 = newEvents.generateOne.copy(id = newEvent.id)
      (waitingEventsGauge.increment _).expects(event2.project.path).returning(IO.unit)

      val nowForEvent2 = Instant.now()
      currentTime.expects().returning(nowForEvent2)

      persister.storeNewEvent(event2).unsafeRunSync() shouldBe Created

      val save2Event1 +: save2Event2 +: Nil = findEvents(status = New)
      save2Event1 shouldBe (newEvent.compoundEventId, ExecutionDate(now), newEvent.batchDate)
      save2Event2 shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2), event2.batchDate)
    }

    "do nothing if there is an event with the same id and project in the db already" in new TestCase {
      val newEvent = newEvents.generateOne

      (waitingEventsGauge.increment _).expects(newEvent.project.path).returning(IO.unit)

      persister.storeNewEvent(newEvent).unsafeRunSync() shouldBe Created

      storedEvent(newEvent.compoundEventId)._1 shouldBe newEvent.compoundEventId

      persister.storeNewEvent(newEvent.copy(body = eventBodies.generateOne)).unsafeRunSync() shouldBe Existed

      storedEvent(newEvent.compoundEventId) shouldBe (
        newEvent.compoundEventId,
        EventStatus.New,
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
    val queriesExecTimes   = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val persister          = new EventPersisterImpl(transactor, waitingEventsGauge, queriesExecTimes, currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now)

    def storedEvent(
        compoundEventId: CompoundEventId
    ): (CompoundEventId, EventStatus, CreatedDate, ExecutionDate, EventDate, EventBody, Option[EventMessage]) =
      execute {
        sql"""select event_id, project_id, status, created_date, execution_date, event_date, event_body, message
             |from event_log  
             |where event_id = ${compoundEventId.id} and project_id = ${compoundEventId.projectId}
         """.stripMargin
          .query[
            (CompoundEventId, EventStatus, CreatedDate, ExecutionDate, EventDate, EventBody, Option[EventMessage])
          ]
          .unique
      }
  }
}
