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

import java.time.Instant.now
import java.time.temporal.ChronoUnit._

import cats.implicits._
import ch.datascience.db.DbSpec
import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.EventsGenerators.{commitIds, projectIds}
import ch.datascience.graph.model.events.{CommitId, ProjectId}
import doobie.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogFetchSpec extends WordSpec with DbSpec with InMemoryEventLogDb with MockFactory {

  import ModelReadsAndWrites._

  "findEventToProcess" should {

    s"find the oldest event with status ${EventStatus.New} and execution date in the past and mark it as PROCESSING" in new TestCase {

      val eventId1   = commitIds.generateOne
      val eventBody1 = eventBodies.generateOne
      storeEvent(eventId1, projectIds.generateOne, EventStatus.New, ExecutionDate(now().minus(5, SECONDS)), eventBody1)

      val eventBody2 = eventBodies.generateOne
      storeEvent(commitIds.generateOne,
                 projectIds.generateOne,
                 EventStatus.New,
                 ExecutionDate(now().plus(5, HOURS)),
                 eventBody2)

      val eventId3   = commitIds.generateOne
      val eventBody3 = eventBodies.generateOne
      storeEvent(eventId3,
                 projectIds.generateOne,
                 EventStatus.TriplesStoreFailure,
                 ExecutionDate(now().minus(5, HOURS)),
                 eventBody3)

      findEvent(EventStatus.Processing) shouldBe List.empty

      concurrentFindEventToProcess.unsafeRunSync()

      findEvent(EventStatus.Processing) shouldBe List(eventId3)

      eventLogFetch.findEventToProcess.unsafeRunSync() shouldBe Some(eventBody1)

      findEvent(EventStatus.Processing) shouldBe List(eventId3, eventId1)

      eventLogFetch.findEventToProcess.unsafeRunSync() shouldBe None
    }

    "find no events when there any matching the criteria" in new TestCase {

      storeEvent(commitIds.generateOne,
                 projectIds.generateOne,
                 EventStatus.New,
                 ExecutionDate(now().plus(5, HOURS)),
                 eventBodies.generateOne)

      eventLogFetch.findEventToProcess.unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {

    val eventLogFetch = new EventLogFetch(transactorProvider)

    def storeEvent(eventId:       CommitId,
                   projectId:     ProjectId,
                   eventStatus:   EventStatus,
                   executionDate: ExecutionDate,
                   eventBody:     EventBody): Unit =
      sql"""insert into 
           |event_log (event_id, project_id, status, created_date, execution_date, event_body) 
           |values ($eventId, $projectId, $eventStatus, ${createdDates.generateOne}, $executionDate, $eventBody)
      """.stripMargin.update.run
        .map(_ => ())
        .transact(transactor)
        .unsafeRunSync()

    def findEvent(eventStatus: EventStatus): List[CommitId] =
      sql"""select event_id
           |from event_log 
           |where status = ${EventStatus.Processing: EventStatus}
         """.stripMargin
        .query[CommitId]
        .to[List]
        .transact(transactor)
        .unsafeRunSync()

    def concurrentFindEventToProcess =
      for {
        _ <- contextShift.shift *> eventLogFetch.findEventToProcess.start
        _ <- contextShift.shift *> eventLogFetch.findEventToProcess
      } yield ()
  }
}
