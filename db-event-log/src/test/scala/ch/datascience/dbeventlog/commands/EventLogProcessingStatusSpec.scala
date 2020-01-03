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

import java.time.Instant
import java.time.temporal.ChronoUnit._

import cats.data.NonEmptyList
import ch.datascience.dbeventlog.DbEventLogGenerators.{eventBodies, eventStatuses, executionDates}
import ch.datascience.dbeventlog.EventStatus._
import ch.datascience.dbeventlog.{CreatedDate, EventStatus}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.{commitEventIds, committedDates, projectIds}
import ch.datascience.graph.model.events.ProjectId
import eu.timepit.refined.auto._
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogProcessingStatusSpec extends WordSpec with InMemoryEventLogDbSpec {

  "fetchStatus" should {

    "return a ProcessingStatus for the given project with " +
      s"$TriplesStore and $NonRecoverableFailure events counted to done " +
      "and all to total" in new TestCase {

      storeEventsWithRecentTime(projectIds.generateOne, nonEmptyList(eventStatuses).generateOne)

      val doneEvents = nonEmptyList(
        Gen.oneOf(TriplesStore, NonRecoverableFailure),
        minElements = 10,
        maxElements = 20
      ).generateOne
      val toBeProcessedEvents = nonEmptyList(
        Gen.oneOf(New, Processing, TriplesStoreFailure),
        minElements = 10,
        maxElements = 20
      ).generateOne
      storeEventsWithRecentTime(projectId, doneEvents ::: toBeProcessedEvents)

      val Some(processingStatus) = processingStatusFinder.fetchStatus(projectId).value.unsafeRunSync()

      val expectedTotal: Int = doneEvents.size + toBeProcessedEvents.size
      processingStatus.done.value           shouldBe doneEvents.size
      processingStatus.total.value          shouldBe expectedTotal
      processingStatus.progress.value.floor shouldBe ((doneEvents.size.toDouble / expectedTotal) * 100).floor
    }

    "return a ProcessingStatus for the given project " +
      "including events only close to the latest created_date" in new TestCase {

      val sameDateEvents = nonEmptyList(eventStatuses, minElements = 10, maxElements = 20).generateOne
      storeEventsWithRecentTime(projectId, sameDateEvents)

      storeEventsWithOlderDate(projectId, nonEmptyList(eventStatuses).generateOne)

      val Some(processingStatus) = processingStatusFinder.fetchStatus(projectId).value.unsafeRunSync()

      processingStatus.total.value shouldBe sameDateEvents.size
    }

    "return a ProcessingStatus for the given project " +
      "even if the last created event was in the past" in new TestCase {

      val newestSameDateEvents = nonEmptyList(eventStatuses, minElements = 10, maxElements = 20).generateOne
      storeEventsWithOlderDate(projectId, newestSameDateEvents)

      storeEventsWithEvenOlderDate(projectId, nonEmptyList(eventStatuses).generateOne)

      val Some(processingStatus) = processingStatusFinder.fetchStatus(projectId).value.unsafeRunSync()

      processingStatus.total.value shouldBe newestSameDateEvents.size
    }

    "return None if there were no events for the project id" in new TestCase {
      processingStatusFinder.fetchStatus(projectId).value.unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    val projectId              = projectIds.generateOne
    val processingStatusFinder = new IOEventLogProcessingStatus(transactor)

    def storeEventsWithRecentTime(projectId: ProjectId, statuses: NonEmptyList[EventStatus]) = statuses map {
      storeEvent(
        commitEventIds.generateOne.copy(projectId = projectId),
        _,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne,
        CreatedDate(Instant.now)
      )
    }

    def storeEventsWithOlderDate(projectId: ProjectId, statuses: NonEmptyList[EventStatus]) = statuses map {
      storeEvent(
        commitEventIds.generateOne.copy(projectId = projectId),
        _,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne,
        CreatedDate(Instant.now.minus(122, SECONDS))
      )
    }

    def storeEventsWithEvenOlderDate(projectId: ProjectId, statuses: NonEmptyList[EventStatus]) = statuses map {
      storeEvent(
        commitEventIds.generateOne.copy(projectId = projectId),
        _,
        executionDates.generateOne,
        committedDates.generateOne,
        eventBodies.generateOne,
        CreatedDate(Instant.now.minus(2, DAYS))
      )
    }
  }
}
