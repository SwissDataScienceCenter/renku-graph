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

package ch.datascience.dbeventlog.creation

import java.time.Instant

import ch.datascience.dbeventlog._
import DbEventLogGenerators._
import EventPersister.Result
import Result._
import ch.datascience.dbeventlog.EventStatus.New
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events.{CompoundEventId, EventBody}
import doobie.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventPersisterSpec extends WordSpec with InMemoryEventLogDbSpec with MockFactory with TypesSerializers {

  "storeNewEvent" should {

    "add a new event if there is no event with the given id for the given project " +
      "and there's no batch waiting or under processing" in new TestCase {

      // storeNewEvent 1
      eventLogAdd.storeNewEvent(event).unsafeRunSync shouldBe Created

      storedEvent(event.compoundEventId) shouldBe (
        event.compoundEventId,
        EventStatus.New,
        CreatedDate(now),
        ExecutionDate(now),
        event.date,
        event.body,
        None
      )

      // storeNewEvent 2 - different event id and different project
      val event2       = events.generateOne
      val nowForEvent2 = Instant.now()
      currentTime.expects().returning(nowForEvent2)
      eventLogAdd.storeNewEvent(event2).unsafeRunSync shouldBe Created

      val save2Event1 +: save2Event2 +: Nil = findEvents(status = New)
      save2Event1 shouldBe (event.compoundEventId, ExecutionDate(now), event.batchDate)
      save2Event2 shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2), event2.batchDate)
    }

    "add a new event if there is no event with the given id for the given project " +
      "and there's a batch for that project waiting and under processing" in new TestCase {

      // storeNewEvent 1
      eventLogAdd.storeNewEvent(event).unsafeRunSync shouldBe Created

      findEvents(status = New).head shouldBe (
        event.compoundEventId, ExecutionDate(now), event.batchDate
      )

      // storeNewEvent 2 - different event id and batch date but same project
      val event2       = event.copy(id = eventIds.generateOne, batchDate = batchDates.generateOne)
      val nowForEvent2 = Instant.now()
      currentTime.expects().returning(nowForEvent2)
      eventLogAdd.storeNewEvent(event2).unsafeRunSync shouldBe Created

      val save2Event1 +: save2Event2 +: Nil = findEvents(status = New)
      save2Event1 shouldBe (event.compoundEventId, ExecutionDate(now), event.batchDate)
      save2Event2 shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2), event.batchDate)
    }

    "add a new event if there is another event with the same id but for a different project" in new TestCase {

      // Save 1
      eventLogAdd.storeNewEvent(event).unsafeRunSync shouldBe Created

      val save1Event1 +: Nil = findEvents(status = New)
      save1Event1 shouldBe (event.compoundEventId, ExecutionDate(now), event.batchDate)

      // Save 2 - the same event id but different project
      val event2       = events.generateOne.copy(id = event.id)
      val event2Body   = eventBodies.generateOne
      val nowForEvent2 = Instant.now()
      currentTime.expects().returning(nowForEvent2)
      eventLogAdd.storeNewEvent(event2).unsafeRunSync shouldBe Created

      val save2Event1 +: save2Event2 +: Nil = findEvents(status = New)
      save2Event1 shouldBe (event.compoundEventId, ExecutionDate(now), event.batchDate)
      save2Event2 shouldBe (event2.compoundEventId, ExecutionDate(nowForEvent2), event2.batchDate)
    }

    "do nothing if there is an event with the same id and project in the db already" in new TestCase {

      eventLogAdd.storeNewEvent(event).unsafeRunSync shouldBe Created

      storedEvent(event.compoundEventId)._1 shouldBe event.compoundEventId

      eventLogAdd.storeNewEvent(event.copy(body = eventBodies.generateOne)).unsafeRunSync shouldBe Existed

      storedEvent(event.compoundEventId) shouldBe (
        event.compoundEventId,
        EventStatus.New,
        CreatedDate(now),
        ExecutionDate(now),
        event.date,
        event.body,
        None
      )
    }
  }

  private trait TestCase {

    val event = events.generateOne

    val currentTime = mockFunction[Instant]
    val eventLogAdd = new EventPersister(transactor, currentTime)

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
