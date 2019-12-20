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

import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog.{EventMessage, EventStatus, ExecutionDate}
import EventStatus._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.{CommitEventId, CommittedDate}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogReSchedulerSpec extends WordSpec with InMemoryEventLogDbSpec with MockFactory {

  "scheduleEventsForProcessing" should {

    s"set status to $New, execution_date to event_date and clean-up the message on all events" in new TestCase {

      val event1Id   = commitEventIds.generateOne
      val event1Date = committedDates.generateOne
      addEvent(event1Id, EventStatus.Processing, timestampsNotInTheFuture.map(ExecutionDate.apply), event1Date)
      val event2Id   = commitEventIds.generateOne
      val event2Date = committedDates.generateOne
      addEvent(event2Id, EventStatus.Processing, timestampsInTheFuture.map(ExecutionDate.apply), event2Date)
      val event3Id   = commitEventIds.generateOne
      val event3Date = committedDates.generateOne
      addEvent(event3Id, EventStatus.TriplesStore, timestampsNotInTheFuture.map(ExecutionDate.apply), event3Date)
      val event4Id      = commitEventIds.generateOne
      val event4Date    = committedDates.generateOne
      val event4Message = Some(eventMessages.generateOne)
      val event4ExecutionDate: Gen[ExecutionDate] = timestampsNotInTheFuture.map(ExecutionDate.apply)
      addEvent(event4Id, NonRecoverableFailure, event4ExecutionDate, event4Date, event4Message)
      val event5Id   = commitEventIds.generateOne
      val event5Date = committedDates.generateOne
      addEvent(event5Id, EventStatus.New, timestampsNotInTheFuture.map(ExecutionDate.apply), event5Date)
      val event6Id   = commitEventIds.generateOne
      val event6Date = committedDates.generateOne
      addEvent(event6Id, TriplesStoreFailure, timestampsNotInTheFuture.map(ExecutionDate.apply), event6Date)

      eventLog
        .scheduleEventsForProcessing()
        .unsafeRunSync() shouldBe ((): Unit)

      findEvents(status = New).toSet shouldBe Set(
        event1Id -> ExecutionDate(event1Date.value),
        event2Id -> ExecutionDate(event2Date.value),
        event3Id -> ExecutionDate(event3Date.value),
        event4Id -> ExecutionDate(event4Date.value),
        event5Id -> ExecutionDate(event5Date.value),
        event6Id -> ExecutionDate(event6Date.value)
      )
      findEventMessage(event4Id) shouldBe None
    }
  }

  private trait TestCase {

    val eventLog = new EventLogReScheduler(transactor)

    def addEvent(commitEventId: CommitEventId,
                 status:        EventStatus,
                 executionDate: Gen[ExecutionDate],
                 committedDate: CommittedDate,
                 maybeMessage:  Option[EventMessage] = None): Unit =
      storeEvent(commitEventId,
                 status,
                 executionDate.generateOne,
                 committedDate,
                 eventBodies.generateOne,
                 projectPath  = projectPaths.generateOne,
                 maybeMessage = maybeMessage)
  }
}
