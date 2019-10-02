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
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogVerifyExistenceSpec extends WordSpec with InMemoryEventLogDbSpec {

  "filterNotExistingInLog" should {

    "return an empty list if an empty list given" in new TestCase {
      storeEvent(
        commitEventIds.generateOne.copy(projectId = projectId),
        eventStatuses.generateOne,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne
      )

      existenceVerification.filterNotExistingInLog(List.empty, projectId).unsafeRunSync() shouldBe List.empty
    }

    "return an empty list if all ids from the list exist in the Log" in new TestCase {
      val eventId1 = commitEventIds.generateOne.copy(projectId = projectId)
      storeEvent(
        eventId1,
        eventStatuses.generateOne,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne
      )
      val eventId2 = commitEventIds.generateOne.copy(projectId = projectId)
      storeEvent(
        eventId2,
        eventStatuses.generateOne,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne
      )

      existenceVerification
        .filterNotExistingInLog(List(eventId1.id, eventId2.id), projectId)
        .unsafeRunSync() shouldBe List.empty
    }

    "return a list with ids which do not exist in the Log for the specified project" in new TestCase {
      val eventId1 = commitEventIds.generateOne.copy(projectId = projectId)
      storeEvent(
        eventId1,
        eventStatuses.generateOne,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne
      )
      val eventId2 = commitEventIds.generateOne
      storeEvent(
        eventId2,
        eventStatuses.generateOne,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne
      )
      val eventId3 = commitEventIds.generateOne.copy(projectId = projectId)
      storeEvent(
        eventId3,
        eventStatuses.generateOne,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne
      )
      val eventId4 = commitEventIds.generateOne.copy(id = eventId1.id)
      storeEvent(
        eventId4,
        eventStatuses.generateOne,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne
      )

      existenceVerification
        .filterNotExistingInLog(List(eventId1.id, eventId2.id), projectId)
        .unsafeRunSync() shouldBe List(eventId2.id)
    }

    "return all the given ids if they belong to a different project" in new TestCase {
      val eventId1 = commitEventIds.generateOne

      storeEvent(
        commitEventIds.generateOne.copy(id = eventId1.id),
        eventStatuses.generateOne,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne
      )

      existenceVerification.filterNotExistingInLog(List(eventId1.id), projectId).unsafeRunSync() shouldBe List(
        eventId1.id
      )
    }
  }

  private trait TestCase {
    val projectId = projectIds.generateOne

    val existenceVerification = new EventLogVerifyExistence(transactor)
  }
}
