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

package ch.datascience.dbeventlog.commands

import java.time.Instant

import ch.datascience.dbeventlog._
import DbEventLogGenerators._
import ch.datascience.dbeventlog.EventStatus.New
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events._
import doobie.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogAddSpec extends WordSpec with InMemoryEventLogDbSpec with MockFactory {

  "storeNewEvent" should {

    "add a new event if there is no event with the given id for the given project" in new TestCase {

      // Save 1
      eventLogAdd.storeNewEvent(commitEvent, eventBody).unsafeRunSync shouldBe ((): Unit)

      storedEvent(commitEvent.commitEventId) shouldBe (
        commitEvent.commitEventId,
        EventStatus.New,
        CreatedDate(now),
        ExecutionDate(now),
        commitEvent.committedDate,
        eventBody,
        None
      )

      // Save 2 - different event id and different project
      val commitEvent2 = commitEvents.generateOne
      val event2Body   = eventBodies.generateOne
      val nowForEvent2 = Instant.now()
      currentTime.expects().returning(nowForEvent2)
      eventLogAdd.storeNewEvent(commitEvent2, event2Body).unsafeRunSync shouldBe ((): Unit)

      val save2Event1 +: save2Event2 +: Nil = findEvents(status = New)
      save2Event1 shouldBe (commitEvent.commitEventId, ExecutionDate(now))
      save2Event2 shouldBe (commitEvent2.commitEventId, ExecutionDate(nowForEvent2))
    }

    "add a new event if there is another event with the same id but for a different project" in new TestCase {

      // Save 1
      eventLogAdd.storeNewEvent(commitEvent, eventBody).unsafeRunSync shouldBe ((): Unit)

      val save1Event1 +: Nil = findEvents(status = New)
      save1Event1 shouldBe (commitEvent.commitEventId, ExecutionDate(now))

      // Save 2 - the same event id but different project
      val commitEvent2 = commitEvents.generateOne.copy(id = commitEvent.id)
      val event2Body   = eventBodies.generateOne
      val nowForEvent2 = Instant.now()
      currentTime.expects().returning(nowForEvent2)
      eventLogAdd.storeNewEvent(commitEvent2, event2Body).unsafeRunSync shouldBe ((): Unit)

      val save2Event1 +: save2Event2 +: Nil = findEvents(status = New)
      save2Event1 shouldBe (commitEvent.commitEventId, ExecutionDate(now))
      save2Event2 shouldBe (commitEvent2.commitEventId, ExecutionDate(nowForEvent2))
    }

    "do nothing if there is an event with the same id and project in the db already" in new TestCase {

      eventLogAdd.storeNewEvent(commitEvent, eventBody).unsafeRunSync shouldBe ((): Unit)

      storedEvent(commitEvent.commitEventId)._1 shouldBe commitEvent.commitEventId

      val otherBody = eventBodies.generateOne
      eventLogAdd.storeNewEvent(commitEvent, otherBody).unsafeRunSync shouldBe ((): Unit)

      storedEvent(commitEvent.commitEventId) shouldBe (
        commitEvent.commitEventId,
        EventStatus.New,
        CreatedDate(now),
        ExecutionDate(now),
        commitEvent.committedDate,
        eventBody,
        None
      )
    }
  }

  private trait TestCase {

    val commitEvent = commitEvents.generateOne
    val eventBody   = eventBodies.generateOne

    val currentTime = mockFunction[Instant]
    val eventLogAdd = new EventLogAdd(transactor, currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now)

    def storedEvent(
        commitEventId: CommitEventId
    ): (CommitEventId, EventStatus, CreatedDate, ExecutionDate, CommittedDate, EventBody, Option[EventMessage]) =
      execute {
        sql"""select event_id, project_id, status, created_date, execution_date, event_date, event_body, message
             |from event_log  
             |where event_id = ${commitEventId.id} and project_id = ${commitEventId.projectId}
         """.stripMargin
          .query[
            (CommitEventId, EventStatus, CreatedDate, ExecutionDate, CommittedDate, EventBody, Option[EventMessage])
          ]
          .unique
      }
  }
}
