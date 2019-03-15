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
import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog.{EventStatus, ExecutionDate}
import EventStatus._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.EventsGenerators.{commitIds, projectIds}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogMarkDoneSpec extends WordSpec with DbSpec with InMemoryEventLogDb with MockFactory {

  "markEventDone" should {

    s"set event with the given id status $TriplesStore " +
      s"if the event has status $Processing" in new TestCase {

      val eventId = commitIds.generateOne
      storeEvent(eventId,
                 projectIds.generateOne,
                 EventStatus.Processing,
                 executionDates.generateOne,
                 eventBodies.generateOne)
      storeEvent(commitIds.generateOne,
                 projectIds.generateOne,
                 EventStatus.Processing,
                 executionDates.generateOne,
                 eventBodies.generateOne)

      eventLogMarkDone.markEventDone(eventId).unsafeRunSync() shouldBe ()

      findEvent(status = TriplesStore) shouldBe List(eventId -> ExecutionDate(currentNow))
    }

    s"fail when updating event with status different than $Processing" in new TestCase {

      val eventId       = commitIds.generateOne
      val eventStatus   = eventStatuses generateDifferentThan Processing
      val executionDate = executionDates.generateOne
      storeEvent(eventId, projectIds.generateOne, eventStatus, executionDate, eventBodies.generateOne)

      intercept[RuntimeException] {
        eventLogMarkDone.markEventDone(eventId).unsafeRunSync()
      }.getMessage shouldBe s"Event with id = $eventId couldn't be updated; Either no event or not with status $Processing"

      findEvent(status = eventStatus) shouldBe List(eventId -> executionDate)
    }
  }

  private trait TestCase {

    val now              = mockFunction[Instant]
    val eventLogMarkDone = new EventLogMarkDone(transactorProvider, now)

    val currentNow = Instant.now()
    now.expects().returning(currentNow).anyNumberOfTimes()
  }
}
