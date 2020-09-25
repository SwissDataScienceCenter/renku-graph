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

package io.renku.eventlog.metrics

import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.{CompoundEventId, EventId}
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.DbEventLogGenerators._
import io.renku.eventlog.EventStatus._
import io.renku.eventlog._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class StatsFinderSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with ScalaCheckPropertyChecks
    with should.Matchers {

  "statuses" should {

    "return info about number of events grouped by status" in {
      forAll { statuses: List[EventStatus] =>
        prepareDbForTest()

        statuses foreach store

        stats.statuses().unsafeRunSync() shouldBe Map(
          New                   -> statuses.count(_ == New),
          Processing            -> statuses.count(_ == Processing),
          TriplesStore          -> statuses.count(_ == TriplesStore),
          Skipped               -> statuses.count(_ == Skipped),
          RecoverableFailure    -> statuses.count(_ == RecoverableFailure),
          NonRecoverableFailure -> statuses.count(_ == NonRecoverableFailure)
        )

        queriesExecTimes.verifyExecutionTimeMeasured("statuses count")
      }
    }
  }

  "countEvents" should {

    "return info about number of events with the given status in the queue grouped by projects" in {
      forAll(nonEmptyList(projectPaths, minElements = 2, maxElements = 8)) { projectPaths =>
        prepareDbForTest()

        val events = generateEventsFor(projectPaths.toList)

        events foreach store

        stats.countEvents(Set(New, RecoverableFailure)).unsafeRunSync() shouldBe events
          .groupBy(_._1)
          .map { case (projectPath, sameProjectGroup) =>
            projectPath -> sameProjectGroup.count { case (_, _, status) =>
              Set(New, RecoverableFailure) contains status
            }
          }
          .filter { case (_, count) =>
            count > 0
          }

        queriesExecTimes.verifyExecutionTimeMeasured("projects events count")
      }
    }
  }

  private val queriesExecTimes = TestLabeledHistogram[SqlQuery.Name]("query_id")
  private val stats            = new StatsFinderImpl(transactor, queriesExecTimes)

  private def store: ((Path, EventId, EventStatus)) => Unit = { case (projectPath, eventId, status) =>
    storeEvent(
      CompoundEventId(eventId, Id(Math.abs(projectPath.value.hashCode))),
      status,
      executionDates.generateOne,
      eventDates.generateOne,
      eventBodies.generateOne,
      projectPath = projectPath
    )
  }

  private def store(status: EventStatus): Unit =
    storeEvent(compoundEventIds.generateOne,
               status,
               executionDates.generateOne,
               eventDates.generateOne,
               eventBodies.generateOne
    )

  private def generateEventsFor(projectPaths: List[Path]) =
    projectPaths flatMap { projectPath =>
      nonEmptyList(eventIdsAndStatuses, maxElements = 20).generateOne.toList.map { case (commitId, status) =>
        (projectPath, commitId, status)
      }
    }

  private val eventIdsAndStatuses =
    for {
      eventId <- eventIds
      status  <- eventStatuses
    } yield eventId -> status
}
