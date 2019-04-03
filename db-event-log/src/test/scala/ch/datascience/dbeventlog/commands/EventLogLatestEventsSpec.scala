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

import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.CommittedDate
import ch.datascience.graph.model.events.EventsGenerators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogLatestEventsSpec extends WordSpec with InMemoryEventLogDbSpec {

  "findAllLatestEvents" should {

    "return an empty list if there are no events in the db" in new TestCase {
      latestEventsFinder.findAllLatestEvents.unsafeRunSync() shouldBe List.empty
    }

    "return (projectId, eventId) tuples with the youngest eventId (event_date wise) " +
      "for all the projects in the db" in new TestCase {
      val projectId1 = projectIds.generateOne
      storeEvent(
        commitEventIds.generateOne.copy(projectId = projectId1),
        eventStatuses.generateOne,
        executionDates.generateOne,
        CommittedDate(now minus (20, DAYS)),
        eventBodies.generateOne
      )

      val youngestCommitEventIdProject1 = commitEventIds.generateOne.copy(projectId = projectId1)
      storeEvent(
        youngestCommitEventIdProject1,
        eventStatuses.generateOne,
        executionDates.generateOne,
        CommittedDate(now minus (3, DAYS)),
        eventBodies.generateOne
      )

      val onlyCommitEventIdProject2 = commitEventIds.generateOne
      storeEvent(
        onlyCommitEventIdProject2,
        eventStatuses.generateOne,
        executionDates.generateOne,
        CommittedDate(now minus (2, DAYS)),
        eventBodies.generateOne
      )

      latestEventsFinder.findAllLatestEvents.unsafeRunSync() shouldBe List(
        youngestCommitEventIdProject1,
        onlyCommitEventIdProject2
      )
    }
  }

  private trait TestCase {
    val latestEventsFinder = new EventLogLatestEvents(transactor)
  }
}
