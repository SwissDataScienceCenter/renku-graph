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
import ch.datascience.graph.model.events.{CommitId, ProjectId}
import doobie.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import DbEventLogGenerators._

class EventLogAddSpec extends WordSpec with DbSpec with InMemoryEventLogDb with MockFactory {

  import ModelReadsAndWrites._

  "storeNewEvent" should {

    "succeed if there is no event with the given id" in new TestCase {

      val currentNow = Instant.now()
      now.expects().returning(currentNow)

      eventLogAdd.storeNewEvent(eventId, projectId, eventBody).unsafeRunSync shouldBe ()

      storedEvent(eventId) shouldBe (
        eventId,
        projectId,
        EventStatus.New,
        CreatedDate(currentNow),
        ExecutionDate(currentNow),
        eventBody,
        None
      )
    }

    "succeed and do nothing if there is event with the given id in the db already" in new TestCase {

      val currentNow = Instant.now()
      now.expects().returning(currentNow)

      eventLogAdd.storeNewEvent(eventId, projectId, eventBody).unsafeRunSync shouldBe ()

      storedEvent(eventId)._1 shouldBe eventId

      val otherBody = eventBodies.generateOne
      eventLogAdd.storeNewEvent(eventId, projectId, otherBody).unsafeRunSync shouldBe ()

      storedEvent(eventId) shouldBe (
        eventId,
        projectId,
        EventStatus.New,
        CreatedDate(currentNow),
        ExecutionDate(currentNow),
        eventBody,
        None
      )
    }
  }

  private trait TestCase {

    val eventId   = commitIds.generateOne
    val projectId = projectIds.generateOne
    val eventBody = eventBodies.generateOne

    val now = mockFunction[Instant]

    val eventLogAdd = new EventLogAdd(transactorProvider, now)

    def storedEvent(
        eventId: CommitId
    ): (CommitId, ProjectId, EventStatus, CreatedDate, ExecutionDate, EventBody, Option[Message]) =
      sql"""select event_id, project_id, status, created_date, execution_date, event_body, message
           |from event_log  
           |where event_id = ${eventId.value}
         """.stripMargin
        .query[(CommitId, ProjectId, EventStatus, CreatedDate, ExecutionDate, EventBody, Option[Message])]
        .unique
        .transact(transactor)
        .unsafeRunSync()
  }
}
