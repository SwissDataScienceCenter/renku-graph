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

package ch.datascience.dbeventlog.commands

import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog._
import EventStatus._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.{CommitEventId, CommitId}
import ch.datascience.graph.model.projects.{Id, Path}
import eu.timepit.refined.auto._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EventLogStatsSpec extends WordSpec with InMemoryEventLogDbSpec with ScalaCheckPropertyChecks {

  "statuses" should {

    "return info about number of events grouped by status" in {
      forAll { statuses: List[EventStatus] =>
        prepareDbForTest()

        statuses foreach store

        stats.statuses.unsafeRunSync() shouldBe Map(
          New                   -> statuses.count(_ == New),
          Processing            -> statuses.count(_ == Processing),
          TriplesStore          -> statuses.count(_ == TriplesStore),
          RecoverableFailure    -> statuses.count(_ == RecoverableFailure),
          NonRecoverableFailure -> statuses.count(_ == NonRecoverableFailure)
        )
      }
    }
  }

  "waitingEvents" should {

    "return info about number of events waiting in the queue split by projects" in {
      forAll(nonEmptyList(projectPaths, minElements = 2, maxElements = 8)) { projectPaths =>
        prepareDbForTest()

        val events = generateEventsFor(projectPaths.toList)

        events foreach store

        stats.waitingEvents.unsafeRunSync() shouldBe events
          .groupBy(_._1)
          .map {
            case (projectPath, sameProjectGroup) => projectPath -> sameProjectGroup.count(_._3 == New)
          }
      }
    }
  }

  private val stats = new EventLogStatsImpl(transactor)

  private def store: ((Path, CommitId, EventStatus)) => Unit = {
    case (projectPath, commitId, status) =>
      storeEvent(
        CommitEventId(commitId, Id(Math.abs(projectPath.value.hashCode))),
        status,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne,
        projectPath = projectPath
      )
  }

  private def store(status: EventStatus): Unit =
    storeEvent(commitEventIds.generateOne,
               status,
               executionDates.generateOne,
               committedDates.generateOne,
               eventBodies.generateOne)

  private def generateEventsFor(projectPaths: List[Path]) =
    projectPaths flatMap { projectPath =>
      nonEmptyList(commitIdsAndStatuses, maxElements = 20).generateOne.toList.map {
        case (commitId, status) => (projectPath, commitId, status)
      }
    }

  private val commitIdsAndStatuses =
    for {
      commitId <- commitIds
      status   <- eventStatuses
    } yield commitId -> status
}
