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

import ch.datascience.db.DbSpec
import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.CommittedDate
import ch.datascience.graph.model.events.EventsGenerators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogLatestEventSpec extends WordSpec with DbSpec with InMemoryEventLogDb {

  "findYoungestEventInLog" should {

    "return None if there are no events for the given projectId" in new TestCase {
      latestEventFinder.findYoungestEventInLog(projectId).unsafeRunSync() shouldBe None
    }

    "return eventId of the event with the youngest event_date for the given projectId" in new TestCase {
      storeEvent(
        commitIds.generateOne,
        projectId,
        eventStatuses.generateOne,
        executionDates.generateOne,
        CommittedDate(now minus (20, DAYS)),
        eventBodies.generateOne
      )

      val youngestEventId = commitIds.generateOne
      storeEvent(
        youngestEventId,
        projectId,
        eventStatuses.generateOne,
        executionDates.generateOne,
        CommittedDate(now minus (3, DAYS)),
        eventBodies.generateOne
      )

      storeEvent(
        commitIds.generateOne,
        projectIds.generateOne,
        eventStatuses.generateOne,
        executionDates.generateOne,
        CommittedDate(now minus (2, DAYS)),
        eventBodies.generateOne
      )

      latestEventFinder.findYoungestEventInLog(projectId).unsafeRunSync() shouldBe Some(youngestEventId)
    }
  }

  private trait TestCase {
    val projectId = projectIds.generateOne

    val latestEventFinder = new EventLogLatestEvent(transactorProvider)
  }
}
