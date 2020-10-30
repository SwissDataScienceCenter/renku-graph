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

package io.renku.eventlog.latestevents

import java.time.Instant.now
import java.time.temporal.ChronoUnit._

import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.DbEventLogGenerators._
import io.renku.eventlog.{EventDate, InMemoryEventLogDbSpec}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class LatestEventsFinderSpec extends AnyWordSpec with InMemoryEventLogDbSpec with should.Matchers {

  "findAllLatestEvents" should {

    "return an empty list if there are no events in the db" in new TestCase {
      latestEventsFinder.findAllLatestEvents().unsafeRunSync() shouldBe List.empty
    }

    "return (projectId, eventBody) tuples with the youngest eventId (event_date wise) " +
      "for all the projects in the db" in new TestCase {
        val project1 = eventProjects.generateOne
        storeEvent(
          compoundEventIds.generateOne.copy(projectId = project1.id),
          eventStatuses.generateOne,
          executionDates.generateOne,
          EventDate(now.minus(20, DAYS)),
          eventBodies.generateOne,
          projectPath = project1.path
        )

        val youngestEventIdProject1   = compoundEventIds.generateOne.copy(projectId = project1.id)
        val youngestEventBodyProject1 = eventBodies.generateOne
        storeEvent(
          youngestEventIdProject1,
          eventStatuses.generateOne,
          executionDates.generateOne,
          EventDate(now.minus(3, DAYS)),
          youngestEventBodyProject1,
          projectPath = project1.path
        )

        val project2          = eventProjects.generateOne
        val eventIdProject2   = compoundEventIds.generateOne.copy(projectId = project2.id)
        val eventBodyProject2 = eventBodies.generateOne
        storeEvent(
          eventIdProject2,
          eventStatuses.generateOne,
          executionDates.generateOne,
          EventDate(now.minus(2, DAYS)),
          eventBodyProject2,
          projectPath = project2.path
        )

        latestEventsFinder.findAllLatestEvents().unsafeRunSync() shouldBe List(
          (youngestEventIdProject1.id, project1, youngestEventBodyProject1),
          (eventIdProject2.id, project2, eventBodyProject2)
        )

        queriesExecTimes.verifyExecutionTimeMeasured("latest projects events")
      }
  }

  private trait TestCase {
    val queriesExecTimes   = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val latestEventsFinder = new LatestEventsFinderImpl(transactor, queriesExecTimes)
  }
}
