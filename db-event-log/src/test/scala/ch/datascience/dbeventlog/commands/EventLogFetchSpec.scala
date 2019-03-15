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
import java.time.temporal.ChronoUnit._

import cats.implicits._
import ch.datascience.db.DbSpec
import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog._
import EventStatus._
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

    "find event with execution date farthest in the past " +
      s"and status $New or $TriplesStoreFailure " +
      s"and mark it as $Processing" in new TestCase {

      val eventId1   = commitIds.generateOne
      val eventBody1 = eventBodies.generateOne
      storeEvent(eventId1,
                 projectIds.generateOne,
                 EventStatus.New,
                 ExecutionDate(currentNow minus (5, SECONDS)),
                 eventBody1)

      val eventBody2 = eventBodies.generateOne
      storeEvent(commitIds.generateOne,
                 projectIds.generateOne,
                 EventStatus.New,
                 ExecutionDate(currentNow plus (5, HOURS)),
                 eventBody2)

      val eventId3   = commitIds.generateOne
      val eventBody3 = eventBodies.generateOne
      storeEvent(eventId3,
                 projectIds.generateOne,
                 EventStatus.TriplesStoreFailure,
                 ExecutionDate(currentNow minus (5, HOURS)),
                 eventBody3)

      findEvent(EventStatus.Processing) shouldBe List.empty

      concurrentFindEventToProcess.unsafeRunSync()

      findEvent(EventStatus.Processing) shouldBe List(eventId3 -> currentNow)

      eventLogFetch.findEventToProcess.unsafeRunSync() shouldBe Some(eventBody1)

      findEvent(EventStatus.Processing) shouldBe List(eventId3 -> currentNow, eventId1 -> currentNow)

      eventLogFetch.findEventToProcess.unsafeRunSync() shouldBe None
    }

    s"find event with the $Processing status " +
      "and execution date older than 10 mins" in new TestCase {

      val eventId   = commitIds.generateOne
      val eventBody = eventBodies.generateOne
      storeEvent(eventId,
                 projectIds.generateOne,
                 EventStatus.Processing,
                 ExecutionDate(currentNow minus (11, MINUTES)),
                 eventBody)

      eventLogFetch.findEventToProcess.unsafeRunSync() shouldBe Some(eventBody)

      findEvent(EventStatus.Processing) shouldBe List(eventId -> currentNow)
    }

    s"find no event when there there's one with $Processing status " +
      "but execution date from less than from 10 mins ago" in new TestCase {

      storeEvent(commitIds.generateOne,
                 projectIds.generateOne,
                 EventStatus.Processing,
                 ExecutionDate(currentNow minus (9, MINUTES)),
                 eventBodies.generateOne)

      eventLogFetch.findEventToProcess.unsafeRunSync() shouldBe None
    }

    "find no events when there any matching the criteria" in new TestCase {

      storeEvent(commitIds.generateOne,
                 projectIds.generateOne,
                 EventStatus.New,
                 ExecutionDate(currentNow plus (5, HOURS)),
                 eventBodies.generateOne)

      eventLogFetch.findEventToProcess.unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {

    val now           = mockFunction[Instant]
    val eventLogFetch = new EventLogFetch(transactorProvider, now)

    val currentNow = Instant.now()
    now.expects().returning(currentNow).anyNumberOfTimes()

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

    def findEvent(eventStatus: EventStatus): List[(CommitId, Instant)] =
      sql"""select event_id, execution_date
           |from event_log 
           |where status = ${Processing: EventStatus}
           |  and execution_date >= ${now() minus (10, MINUTES)}
         """.stripMargin
        .query[(CommitId, Instant)]
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
