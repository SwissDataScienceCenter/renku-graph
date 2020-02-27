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
import ch.datascience.dbeventlog.DbEventLogGenerators.{createdDates, eventBodies, eventStatuses}
import ch.datascience.dbeventlog.EventStatus._
import ch.datascience.dbeventlog.{EventStatus, ExecutionDate}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators.{commitEventIds, committedDates, projectIds}
import ch.datascience.graph.model.events.ProjectId
import eu.timepit.refined.auto._
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Random

class EventLogProcessingStatusSpec extends WordSpec with InMemoryEventLogDbSpec {

  "fetchStatus" should {

    "return ProcessingStatus for the given project " +
      s"where $TriplesStore and $NonRecoverableFailure events are counted as done " +
      "and all as total" in new TestCase {

      storeEventsWithRecentTime(projectIds.generateOne, nonEmptyList(eventStatuses).generateOne)

      val toBeProcessedEvents = nonEmptyList(
        Gen.oneOf(New, Processing, RecoverableFailure),
        minElements = 10,
        maxElements = 20
      ).generateOne
      val doneEvents = nonEmptyList(
        Gen.oneOf(TriplesStore, NonRecoverableFailure),
        minElements = 10,
        maxElements = 20
      ).generateOne
      storeEventsWithRecentTime(projectId, toBeProcessedEvents ::: doneEvents)

      val Some(processingStatus) = processingStatusFinder.fetchStatus(projectId).value.unsafeRunSync()

      val expectedTotal: Int = doneEvents.size + toBeProcessedEvents.size
      processingStatus.done.value           shouldBe doneEvents.size
      processingStatus.total.value          shouldBe expectedTotal
      processingStatus.progress.value.floor shouldBe ((doneEvents.size.toDouble / expectedTotal) * 100).floor
    }

    "return ProcessingStatus " +
      "even if events are spread across longer period of time" in new TestCase {

      val spreadInMinutes:      Int                       = 60 * 24 * 2
      val spreadEventsStatuses: NonEmptyList[EventStatus] = nonEmptyList(eventStatuses).generateOne
      spreadEventsInThePastAndStore(projectId, spreadEventsStatuses, maxMinutesInPast = spreadInMinutes)

      generateEventAndStore(projectId,
                            Gen.oneOf(New, Processing, RecoverableFailure).generateOne,
                            ExecutionDate(Instant.now minus (spreadInMinutes + 1, MINUTES)))

      val Some(processingStatus) = processingStatusFinder.fetchStatus(projectId).value.unsafeRunSync()

      processingStatus.total.value shouldBe spreadEventsStatuses.size + 1
    }

    "return ProcessingStatus with done=total=0 " +
      "if all the events are processed" in new TestCase {

      val eventsStatuses = nonEmptyList(Gen.oneOf(TriplesStore, NonRecoverableFailure)).generateOne
      spreadEventsInThePastAndStore(projectId, eventsStatuses, maxMinutesInPast = positiveInts().generateOne.value)

      val Some(processingStatus) = processingStatusFinder.fetchStatus(projectId).value.unsafeRunSync()

      processingStatus.total.value    shouldBe eventsStatuses.size
      processingStatus.done.value     shouldBe eventsStatuses.size
      processingStatus.progress.value shouldBe 100d
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
        ExecutionDate(Instant.now),
        committedDates.generateOne,
        eventBodies.generateOne,
        createdDates.generateOne
      )
    }

    def spreadEventsInThePastAndStore(projectId:        ProjectId,
                                      statuses:         NonEmptyList[EventStatus],
                                      maxMinutesInPast: Int) = statuses map {
      storeEvent(
        commitEventIds.generateOne.copy(projectId = projectId),
        _,
        ExecutionDate(Instant.now.minus(Random nextInt maxMinutesInPast, MINUTES)),
        committedDates.generateOne,
        eventBodies.generateOne,
        createdDates.generateOne
      )
    }

    def generateEventAndStore(projectId: ProjectId, status: EventStatus, executionDate: ExecutionDate) =
      storeEvent(
        commitEventIds.generateOne.copy(projectId = projectId),
        status,
        executionDate,
        committedDates.generateOne,
        eventBodies.generateOne,
        createdDates.generateOne
      )
  }
}
