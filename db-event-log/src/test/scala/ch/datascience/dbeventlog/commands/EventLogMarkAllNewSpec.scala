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

import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog.{EventStatus, ExecutionDate}
import EventStatus._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events.{CommitEventId, ProjectPath}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogMarkAllNewSpec extends WordSpec with InMemoryEventLogDbSpec with MockFactory {

  "markEventsAsNew" should {

    s"set status to $New and execution_date to now " +
      s"on all events with status $Processing, $TriplesStore and $TriplesStoreFailure " +
      "and execution_date in the past" in new TestCase {

      val event1Id     = commitEventIds.generateOne
      val projectPath1 = projectPaths.generateOne
      addEvent(event1Id, projectPath1, EventStatus.Processing, timestampsNotInTheFuture.map(ExecutionDate.apply))
      val event2Id = commitEventIds.generateOne.copy(projectId = event1Id.projectId)
      addEvent(event2Id, projectPath1, EventStatus.Processing, timestampsInTheFuture.map(ExecutionDate.apply))
      val event3Id = commitEventIds.generateOne.copy(projectId = event1Id.projectId)
      addEvent(event3Id, projectPath1, EventStatus.TriplesStore, timestampsNotInTheFuture.map(ExecutionDate.apply))
      val event4Id     = commitEventIds.generateOne
      val projectPath4 = projectPaths.generateOne
      addEvent(event4Id, projectPath4, NonRecoverableFailure, timestampsNotInTheFuture.map(ExecutionDate.apply))
      val event5Id     = commitEventIds.generateOne
      val projectPath5 = projectPaths.generateOne
      addEvent(event5Id, projectPath5, EventStatus.New, timestampsNotInTheFuture.map(ExecutionDate.apply))
      val event6Id = commitEventIds.generateOne.copy(projectId = event5Id.projectId)
      addEvent(event6Id, projectPath5, TriplesStoreFailure, timestampsNotInTheFuture.map(ExecutionDate.apply))

      eventLogMarkAllNew
        .markEventsAsNew(projectPath1, Set(event1Id.id, event2Id.id, event3Id.id))
        .unsafeRunSync() shouldBe ((): Unit)
      eventLogMarkAllNew
        .markEventsAsNew(projectPath4, Set(event4Id.id))
        .unsafeRunSync() shouldBe ((): Unit)
      eventLogMarkAllNew
        .markEventsAsNew(projectPath5, Set(event5Id.id, event6Id.id))
        .unsafeRunSync() shouldBe ((): Unit)

      findEvents(status = New).toSet shouldBe Set(event1Id -> ExecutionDate(now),
                                                  event3Id -> ExecutionDate(now),
                                                  event5Id -> ExecutionDate(now),
                                                  event6Id -> ExecutionDate(now))
    }

    "do nothing when no events got updated" in new TestCase {

      val eventId     = commitEventIds.generateOne
      val projectPath = projectPaths.generateOne
      addEvent(eventId, projectPath, EventStatus.Processing, timestampsInTheFuture.map(ExecutionDate.apply))

      eventLogMarkAllNew.markEventsAsNew(projectPath, Set(eventId.id)).unsafeRunSync() shouldBe ((): Unit)

      findEvents(status = New) shouldBe Nil
    }
  }

  private trait TestCase {

    val currentTime        = mockFunction[Instant]
    val eventLogMarkAllNew = new EventLogMarkAllNew(transactor, currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()

    def addEvent(commitEventId: CommitEventId,
                 projectPath:   ProjectPath,
                 status:        EventStatus,
                 executionDate: Gen[ExecutionDate]): Unit =
      storeEvent(commitEventId,
                 status,
                 executionDate.generateOne,
                 committedDates.generateOne,
                 eventBodies.generateOne,
                 projectPath = projectPath)
  }
}
