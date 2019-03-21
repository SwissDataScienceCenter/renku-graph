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
import ch.datascience.dbeventlog.commands.EventLogLatestEvents.LatestEvent
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.CommittedDate
import ch.datascience.graph.model.events.EventsGenerators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogLatestEventsSpec extends WordSpec with DbSpec with InMemoryEventLogDb {

  "findAllLatestEvents" should {

    "return an empty list if there are no events in the db" in new TestCase {
      latestEventsFinder.findAllLatestEvents.unsafeRunSync() shouldBe List.empty
    }

    "return (projectId, eventId) tuples with the youngest eventId (event_date wise) " +
      "for all the projects in the db" in new TestCase {
      val projectId1 = projectIds.generateOne
      storeEvent(
        commitIds.generateOne,
        projectId1,
        eventStatuses.generateOne,
        executionDates.generateOne,
        CommittedDate(now minus (20, DAYS)),
        eventBodies.generateOne
      )

      val youngestEventIdProjectId1 = commitIds.generateOne
      storeEvent(
        youngestEventIdProjectId1,
        projectId1,
        eventStatuses.generateOne,
        executionDates.generateOne,
        CommittedDate(now minus (3, DAYS)),
        eventBodies.generateOne
      )

      val eventIdProjectId2 = commitIds.generateOne
      val projectId2        = projectIds.generateOne
      storeEvent(
        eventIdProjectId2,
        projectId2,
        eventStatuses.generateOne,
        executionDates.generateOne,
        CommittedDate(now minus (2, DAYS)),
        eventBodies.generateOne
      )

      latestEventsFinder.findAllLatestEvents.unsafeRunSync() shouldBe List(
        LatestEvent(projectId1, youngestEventIdProjectId1),
        LatestEvent(projectId2, eventIdProjectId2)
      )
    }
  }

  private trait TestCase {
    val latestEventsFinder = new EventLogLatestEvents(transactorProvider)
  }
}
