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
import ch.datascience.graph.model.EventsGenerators.{commitEventIds, committedDates}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogMarkNewSpec extends WordSpec with InMemoryEventLogDbSpec with MockFactory {

  "markEventNew" should {

    s"set status $New on the event with the given id and project " +
      s"if the event has status $Processing" in new TestCase {

      val eventId = commitEventIds.generateOne
      storeEvent(eventId,
                 EventStatus.Processing,
                 executionDates.generateOne,
                 committedDates.generateOne,
                 eventBodies.generateOne)
      storeEvent(commitEventIds.generateOne.copy(id = eventId.id),
                 EventStatus.Processing,
                 executionDates.generateOne,
                 committedDates.generateOne,
                 eventBodies.generateOne)
      storeEvent(commitEventIds.generateOne,
                 EventStatus.Processing,
                 executionDates.generateOne,
                 committedDates.generateOne,
                 eventBodies.generateOne)

      eventLogMarkNew.markEventNew(eventId).unsafeRunSync() shouldBe ((): Unit)

      findEvents(status = New) shouldBe List((eventId, ExecutionDate(now)))
    }

    s"fail when updating event with status different than $Processing" in new TestCase {

      val eventId       = commitEventIds.generateOne
      val eventStatus   = eventStatuses generateDifferentThan Processing
      val executionDate = executionDates.generateOne
      storeEvent(eventId, eventStatus, executionDate, committedDates.generateOne, eventBodies.generateOne)

      intercept[RuntimeException] {
        eventLogMarkNew.markEventNew(eventId).unsafeRunSync()
      }.getMessage shouldBe s"Event with $eventId couldn't be marked as $New; either no event or not with status $Processing"

      findEvents(status = eventStatus) shouldBe List((eventId, executionDate))
    }
  }

  private trait TestCase {

    val currentTime     = mockFunction[Instant]
    val eventLogMarkNew = new EventLogMarkNew(transactor, currentTime)

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()
  }
}
