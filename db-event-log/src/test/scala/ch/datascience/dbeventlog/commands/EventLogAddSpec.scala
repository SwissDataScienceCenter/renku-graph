/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import ch.datascience.db.DbSpec
import ch.datascience.dbeventlog._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events.{CommitId, CommittedDate, ProjectId}
import doobie.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import DbEventLogGenerators._

class EventLogAddSpec extends WordSpec with DbSpec with InMemoryEventLogDb with MockFactory {

  "storeNewEvent" should {

    "succeed if there is no event with the given id" in new TestCase {

      eventLogAdd.storeNewEvent(commitEvent, eventBody).unsafeRunSync shouldBe ()

      storedEvent(eventId) shouldBe (
        commitEvent.id,
        commitEvent.project.id,
        EventStatus.New,
        CreatedDate(currentNow),
        ExecutionDate(currentNow),
        commitEvent.committedDate,
        eventBody,
        None
      )
    }

    "succeed and do nothing if there is event with the given id in the db already" in new TestCase {

      eventLogAdd.storeNewEvent(commitEvent, eventBody).unsafeRunSync shouldBe ()

      storedEvent(eventId)._1 shouldBe eventId

      val otherBody = eventBodies.generateOne
      eventLogAdd.storeNewEvent(commitEvent, otherBody).unsafeRunSync shouldBe ()

      storedEvent(eventId) shouldBe (
        commitEvent.id,
        commitEvent.project.id,
        EventStatus.New,
        CreatedDate(currentNow),
        ExecutionDate(currentNow),
        commitEvent.committedDate,
        eventBody,
        None
      )
    }
  }

  private trait TestCase {

    val commitEvent = commitEvents.generateOne
    val eventId     = commitEvent.id
    val eventBody   = eventBodies.generateOne

    val now         = mockFunction[Instant]
    val eventLogAdd = new EventLogAdd(transactorProvider, now)

    val currentNow = Instant.now()
    now.expects().returning(currentNow)

    def storedEvent(
        eventId: CommitId
    ): (CommitId, ProjectId, EventStatus, CreatedDate, ExecutionDate, CommittedDate, EventBody, Option[EventMessage]) =
      sql"""select event_id, project_id, status, created_date, execution_date, event_date, event_body, message
           |from event_log  
           |where event_id = ${eventId.value}
         """.stripMargin
        .query[(CommitId,
                ProjectId,
                EventStatus,
                CreatedDate,
                ExecutionDate,
                CommittedDate,
                EventBody,
                Option[EventMessage])]
        .unique
        .transact(transactor)
        .unsafeRunSync()
  }
}
