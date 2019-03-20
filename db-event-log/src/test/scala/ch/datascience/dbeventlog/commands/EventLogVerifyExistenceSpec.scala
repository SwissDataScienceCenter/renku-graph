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

import ch.datascience.db.DbSpec
import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.EventsGenerators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogVerifyExistenceSpec extends WordSpec with DbSpec with InMemoryEventLogDb {

  "filterNotExistingInLog" should {

    "return an empty list if an empty list given" in new TestCase {
      storeEvent(
        commitIds.generateOne,
        projectId,
        eventStatuses.generateOne,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne
      )

      existenceVerification.filterNotExistingInLog(List.empty, projectId).unsafeRunSync() shouldBe List.empty
    }

    "return an empty list if all ids from the list exist in the Log" in new TestCase {
      val eventId1 = commitIds.generateOne
      storeEvent(
        eventId1,
        projectId,
        eventStatuses.generateOne,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne
      )
      val eventId2 = commitIds.generateOne
      storeEvent(
        eventId2,
        projectId,
        eventStatuses.generateOne,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne
      )

      existenceVerification
        .filterNotExistingInLog(List(eventId1, eventId2), projectId)
        .unsafeRunSync() shouldBe List.empty
    }

    "return a list with ids which do not exist in the Log" in new TestCase {
      val eventId1 = commitIds.generateOne
      storeEvent(
        eventId1,
        projectId,
        eventStatuses.generateOne,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne
      )
      val eventId2 = commitIds.generateOne
      storeEvent(
        eventId2,
        projectIds.generateOne,
        eventStatuses.generateOne,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne
      )
      val eventId3 = commitIds.generateOne
      storeEvent(
        eventId3,
        projectId,
        eventStatuses.generateOne,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne
      )

      existenceVerification
        .filterNotExistingInLog(List(eventId1, eventId2), projectId)
        .unsafeRunSync() shouldBe List(eventId2)
    }

    "return all the given ids if there are they belongs to a different project" in new TestCase {
      val eventId1 = commitIds.generateOne

      storeEvent(
        commitIds.generateOne,
        projectIds.generateOne,
        eventStatuses.generateOne,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne
      )

      existenceVerification.filterNotExistingInLog(List(eventId1), projectId).unsafeRunSync() shouldBe List(eventId1)
    }
  }

  private trait TestCase {
    val projectId = projectIds.generateOne

    val existenceVerification = new EventLogVerifyExistence(transactorProvider)
  }
}
