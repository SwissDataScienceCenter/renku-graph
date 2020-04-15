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

package ch.datascience.dbeventlog.statuschange.commands

import java.time.Instant

import ch.datascience.dbeventlog.DbEventLogGenerators.{eventDates, eventStatuses, executionDates}
import ch.datascience.dbeventlog.EventStatus.{New, Processing}
import ch.datascience.dbeventlog.commands.InMemoryEventLogDbSpec
import ch.datascience.dbeventlog.statuschange.UpdateCommandsRunner
import ch.datascience.dbeventlog.{EventStatus, ExecutionDate}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{batchDates, compoundEventIds, eventBodies}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class ToNewSpec extends WordSpec with InMemoryEventLogDbSpec with MockFactory {

  "command" should {

    s"set status $New on the event with the given id and $Processing status " +
      s"and return ${UpdateResult.Updated}" in new TestCase {

      storeEvent(
        eventId,
        EventStatus.Processing,
        executionDates.generateOne,
        eventDates.generateOne,
        eventBodies.generateOne,
        batchDate = eventBatchDate
      )
      storeEvent(
        compoundEventIds.generateOne.copy(id = eventId.id),
        EventStatus.Processing,
        executionDates.generateOne,
        eventDates.generateOne,
        eventBodies.generateOne,
        batchDate = eventBatchDate
      )
      storeEvent(
        compoundEventIds.generateOne,
        EventStatus.Processing,
        executionDates.generateOne,
        eventDates.generateOne,
        eventBodies.generateOne,
        batchDate = eventBatchDate
      )

      findEvents(status = New) shouldBe List.empty

      val command = ToNew(eventId, currentTime)

      (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.Updated

      findEvents(status = New) shouldBe List((eventId, ExecutionDate(now), eventBatchDate))
    }

    EventStatus.all.filterNot(_ == Processing) foreach { eventStatus =>
      s"do nothing when updating event with $eventStatus status " +
        s"and return ${UpdateResult.Conflict}" in new TestCase {

        val eventStatus   = eventStatuses generateDifferentThan Processing
        val executionDate = executionDates.generateOne
        storeEvent(eventId,
                   eventStatus,
                   executionDate,
                   eventDates.generateOne,
                   eventBodies.generateOne,
                   batchDate = eventBatchDate)

        findEvents(status = eventStatus) shouldBe List((eventId, executionDate, eventBatchDate))

        val command = ToNew(eventId, currentTime)

        (commandRunner run command).unsafeRunSync() shouldBe UpdateResult.Conflict

        val expectedEvents =
          if (eventStatus != New) List.empty
          else List((eventId, executionDate, eventBatchDate))
        findEvents(status = New) shouldBe expectedEvents
      }
    }
  }

  private trait TestCase {
    val currentTime    = mockFunction[Instant]
    val eventId        = compoundEventIds.generateOne
    val eventBatchDate = batchDates.generateOne

    val commandRunner = new UpdateCommandsRunner(transactor)

    val now = Instant.now()
    currentTime.expects().returning(now).anyNumberOfTimes()
  }
}
