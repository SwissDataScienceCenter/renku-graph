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
import ch.datascience.dbeventlog._
import EventStatus._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.{commitEventIds, committedDates}
import ch.datascience.graph.model.events.CommitEventId
import doobie.implicits._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogMarkFailedSpec extends WordSpec with InMemoryEventLogDbSpec with MockFactory {

  import ExecutionDateCalculator._

  "markEventFailed" should {

    s"set the given $RecoverableFailure status and message on event with the given id and project " +
      s"if the event has status $Processing" in new TestCase {

      storeEvent(commitEventIds.generateOne.copy(id = eventId.id),
                 EventStatus.Processing,
                 executionDate,
                 committedDates.generateOne,
                 eventBodies.generateOne,
                 createdDate)
      storeEvent(eventId,
                 EventStatus.Processing,
                 executionDate,
                 committedDates.generateOne,
                 eventBodies.generateOne,
                 createdDate)

      val maybeMessage     = Gen.option(eventMessages).generateOne
      val newExecutionDate = executionDates.generateOne
      (executionDateCalculator
        .newExecutionDate(_: CreatedDate, _: ExecutionDate)(_: StatusBasedCalculator[RecoverableFailure]))
        .expects(createdDate, executionDate, recoverableFailureCalculator)
        .returning(newExecutionDate)

      eventLogMarkFailed
        .markEventFailed(eventId, RecoverableFailure, maybeMessage)
        .unsafeRunSync() shouldBe ((): Unit)

      findEvent(eventId) shouldBe (newExecutionDate, RecoverableFailure, maybeMessage)
    }

    s"set the given $NonRecoverableFailure status and message on event with the given id and project " +
      s"if the event has status $Processing" in new TestCase {

      storeEvent(commitEventIds.generateOne.copy(id = eventId.id),
                 EventStatus.Processing,
                 executionDate,
                 committedDates.generateOne,
                 eventBodies.generateOne,
                 createdDate)
      storeEvent(eventId,
                 EventStatus.Processing,
                 executionDate,
                 committedDates.generateOne,
                 eventBodies.generateOne,
                 createdDate)

      val maybeMessage     = nestedExceptions.map(EventMessage.apply).generateOne
      val newExecutionDate = executionDates.generateOne
      (executionDateCalculator
        .newExecutionDate(_: CreatedDate, _: ExecutionDate)(_: StatusBasedCalculator[NonRecoverableFailure]))
        .expects(createdDate, executionDate, nonRecoverableFailureCalculator)
        .returning(newExecutionDate)

      eventLogMarkFailed
        .markEventFailed(eventId, NonRecoverableFailure, maybeMessage)
        .unsafeRunSync() shouldBe ((): Unit)

      findEvent(eventId) shouldBe (newExecutionDate, NonRecoverableFailure, maybeMessage)
    }

    s"do nothing when setting $RecoverableFailure and event status is different than $Processing" in new TestCase {

      val eventStatus = eventStatuses generateDifferentThan Processing
      storeEvent(eventId, eventStatus, executionDate, committedDates.generateOne, eventBodies.generateOne, createdDate)

      val message          = eventMessages.generateOne
      val newExecutionDate = executionDates.generateOne
      (executionDateCalculator
        .newExecutionDate(_: CreatedDate, _: ExecutionDate)(_: StatusBasedCalculator[RecoverableFailure]))
        .expects(createdDate, executionDate, recoverableFailureCalculator)
        .returning(newExecutionDate)

      eventLogMarkFailed
        .markEventFailed(eventId, RecoverableFailure, Some(message))
        .unsafeRunSync() shouldBe ((): Unit)

      findEvent(eventId) shouldBe (executionDate, eventStatus, None)
    }

    s"do nothing when setting $NonRecoverableFailure and event status is different than $Processing" in new TestCase {

      val eventStatus = eventStatuses generateDifferentThan Processing
      storeEvent(eventId, eventStatus, executionDate, committedDates.generateOne, eventBodies.generateOne, createdDate)

      val message          = eventMessages.generateOne
      val newExecutionDate = executionDates.generateOne
      (executionDateCalculator
        .newExecutionDate(_: CreatedDate, _: ExecutionDate)(_: StatusBasedCalculator[NonRecoverableFailure]))
        .expects(createdDate, executionDate, nonRecoverableFailureCalculator)
        .returning(newExecutionDate)

      eventLogMarkFailed
        .markEventFailed(eventId, NonRecoverableFailure, Some(message))
        .unsafeRunSync() shouldBe ((): Unit)

      findEvent(eventId) shouldBe (executionDate, eventStatus, None)
    }
  }

  private trait TestCase {

    val eventId       = commitEventIds.generateOne
    val createdDate   = createdDates.generateOne
    val executionDate = executionDates.generateOne

    val executionDateCalculator = mock[ExecutionDateCalculator]
    val eventLogMarkFailed      = new EventLogMarkFailed(transactor, executionDateCalculator)

    def findEvent(eventId: CommitEventId): (ExecutionDate, EventStatus, Option[EventMessage]) = execute {
      sql"""select execution_date, status, message
           |from event_log 
           |where event_id = ${eventId.id} and project_id = ${eventId.projectId}
         """.stripMargin
        .query[(ExecutionDate, EventStatus, Option[EventMessage])]
        .unique
    }
  }
}
