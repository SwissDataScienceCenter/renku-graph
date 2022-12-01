/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events

import EventsEndpoint.Criteria._
import EventsEndpoint._
import cats.effect.IO
import cats.syntax.all._
import io.renku.db.SqlStatement
import io.renku.graph.model.EventContentGenerators._
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.fixed
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.events.CompoundEventId
import io.renku.http.rest.SortBy
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class EventsFinderSpec extends AnyWordSpec with IOSpec with InMemoryEventLogDbSpec with should.Matchers {

  "findEvents" should {

    "return the List of events of the project the given path" in new TestCase {
      val projectId = projectIds.generateOne
      val infos     = eventInfos(fixed(projectPath), fixed(projectId)).generateList(max = 10)

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, projectId)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.project.path,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }
      storeGeneratedEvent(
        eventStatuses.generateOne,
        eventDates.generateOne,
        projectIds.generateOne,
        projectPaths.generateOne
      )

      val pagedResults = eventsFinder
        .findEvents(Criteria(Filters.ProjectEvents(projectPath, maybeStatus = None, maybeDates = None)))
        .unsafeRunSync()

      pagedResults.pagingInfo.total.value   shouldBe infos.size
      pagedResults.pagingInfo.pagingRequest shouldBe PagingRequest.default
      pagedResults.results                  shouldBe infos.sortBy(_.eventDate).reverse
    }

    "return the List of events of the project with the given path and the given PagingRequest" in new TestCase {
      val projectId = projectIds.generateOne
      val infos     = eventInfos(fixed(projectPath), fixed(projectId)).generateList(min = 3, max = 10)

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, projectId)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.project.path,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val pagingRequest: PagingRequest = PagingRequest(Page(2), PerPage(1))
      val pagedResults = eventsFinder
        .findEvents(
          Criteria(Filters.ProjectEvents(projectPath, maybeStatus = None, maybeDates = None), paging = pagingRequest)
        )
        .unsafeRunSync()

      pagedResults.pagingInfo.total.value   shouldBe infos.size
      pagedResults.pagingInfo.pagingRequest shouldBe pagingRequest
      pagedResults.results                  shouldBe List(infos.sortBy(_.eventDate).reverse.tail.head)
    }

    "return the List of events of the project with the given path and the given direction" in new TestCase {
      val projectId = projectIds.generateOne
      val infos     = eventInfos(fixed(projectPath), fixed(projectId)).generateList(min = 3, max = 10)

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, projectId)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.project.path,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val pagingRequest = PagingRequest(Page(1), PerPage(3))
      val pagedResults = eventsFinder
        .findEvents(
          Criteria(Filters.ProjectEvents(projectPath, maybeStatus = None, maybeDates = None),
                   Sorting.By(Sorting.EventDate, SortBy.Direction.Asc),
                   pagingRequest
          )
        )
        .unsafeRunSync()

      pagedResults.pagingInfo.total.value   shouldBe infos.size
      pagedResults.pagingInfo.pagingRequest shouldBe pagingRequest
      pagedResults.results                  shouldBe infos.sortBy(_.eventDate).take(3)

      val ascPagedResults = eventsFinder
        .findEvents(
          Criteria(Filters.ProjectEvents(projectPath, maybeStatus = None, maybeDates = None),
                   Sorting.By(Sorting.EventDate, SortBy.Direction.Desc),
                   pagingRequest
          )
        )
        .unsafeRunSync()

      ascPagedResults.pagingInfo.total.value   shouldBe infos.size
      ascPagedResults.pagingInfo.pagingRequest shouldBe pagingRequest
      ascPagedResults.results                  shouldBe infos.sortBy(_.eventDate).reverse.take(3)
    }

    "return the List of events of the project with the given path and status filter" in new TestCase {
      val projectId = projectIds.generateOne
      val infos     = eventInfos(fixed(projectPath), fixed(projectId)).generateList(min = 3, max = 10)

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, projectId)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.project.path,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val eventStatusFilter = Random.shuffle(infos.map(_.status)).head
      val pagedResults = eventsFinder
        .findEvents(Criteria(Filters.ProjectEvents(projectPath, eventStatusFilter.some, maybeDates = None)))
        .unsafeRunSync()

      val expectedResult = infos.filter(_.status == eventStatusFilter)
      pagedResults.pagingInfo.total.value   shouldBe expectedResult.size
      pagedResults.pagingInfo.pagingRequest shouldBe PagingRequest.default
      pagedResults.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
    }

    "return the List of events of the project with the given path and since filters" in new TestCase {
      val projectId = projectIds.generateOne
      val infos     = eventInfos(fixed(projectPath), fixed(projectId)).generateList(min = 3, max = 10)

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, projectId)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.project.path,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val sinceFilter = Random.shuffle(infos.map(_.eventDate)).head
      val pagedResults = eventsFinder
        .findEvents(
          Criteria(
            Filters.ProjectEvents(projectPath, maybeStatus = None, Filters.EventsSince(sinceFilter).some)
          )
        )
        .unsafeRunSync()

      val expectedResult = infos.filter(_.eventDate.value.compareTo(sinceFilter.value) >= 0)
      pagedResults.pagingInfo.total.value   shouldBe expectedResult.size
      pagedResults.pagingInfo.pagingRequest shouldBe PagingRequest.default
      pagedResults.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
    }

    "return the List of events of the project with the given path and until filters" in new TestCase {
      val projectId = projectIds.generateOne
      val infos     = eventInfos(fixed(projectPath), fixed(projectId)).generateList(min = 3, max = 10)

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, projectId)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.project.path,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val untilFilter = Random.shuffle(infos.map(_.eventDate)).head
      val pagedResults = eventsFinder
        .findEvents(
          Criteria(
            Filters.ProjectEvents(projectPath, maybeStatus = None, Filters.EventsUntil(untilFilter).some)
          )
        )
        .unsafeRunSync()

      val expectedResult = infos.filter(_.eventDate.value.compareTo(untilFilter.value) <= 0)
      pagedResults.pagingInfo.total.value   shouldBe expectedResult.size
      pagedResults.pagingInfo.pagingRequest shouldBe PagingRequest.default
      pagedResults.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
    }

    "return the List of events of all projects matching the given status filter" in new TestCase {

      val infos = eventInfos().generateList(min = 3, max = 10)

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, info.project.id)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.project.path,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val eventStatusFilter = Random.shuffle(infos.map(_.status)).head

      val pagedResults = eventsFinder
        .findEvents(Criteria(Filters.EventsWithStatus(eventStatusFilter, maybeDates = None)))
        .unsafeRunSync()

      val expectedResult = infos.filter(_.status == eventStatusFilter)
      pagedResults.pagingInfo.total.value   shouldBe expectedResult.size
      pagedResults.pagingInfo.pagingRequest shouldBe PagingRequest.default
      pagedResults.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
    }

    "return the List of events of all projects matching the given status and since filters" in new TestCase {

      val status = eventStatuses.generateOne
      val infos  = eventInfos().generateFixedSizeList(ofSize = 3).map(_.copy(status = status))

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, info.project.id)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.project.path,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val _ :: mid :: youngest :: Nil = infos.sortBy(_.eventDate.value)

      val pagedResults = eventsFinder
        .findEvents(
          Criteria(Filters.EventsWithStatus(status, Filters.EventsSince(mid.eventDate).some))
        )
        .unsafeRunSync()

      val expectedResult = List(mid, youngest)
      pagedResults.pagingInfo.total.value   shouldBe expectedResult.size
      pagedResults.pagingInfo.pagingRequest shouldBe PagingRequest.default
      pagedResults.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
    }

    "return the List of events of all projects matching the given status and until filters" in new TestCase {

      val status = eventStatuses.generateOne
      val infos  = eventInfos().generateFixedSizeList(ofSize = 3).map(_.copy(status = status))

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, info.project.id)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.project.path,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val oldest :: mid :: _ :: Nil = infos.sortBy(_.eventDate.value)

      val pagedResults = eventsFinder
        .findEvents(
          Criteria(Filters.EventsWithStatus(status, Filters.EventsUntil(mid.eventDate).some))
        )
        .unsafeRunSync()

      val expectedResult = List(oldest, mid)
      pagedResults.pagingInfo.total.value   shouldBe expectedResult.size
      pagedResults.pagingInfo.pagingRequest shouldBe PagingRequest.default
      pagedResults.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
    }

    "return the List of events of all projects matching the given since filter" in new TestCase {

      val infos = eventInfos().generateList(min = 3, max = 10)

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, info.project.id)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.project.path,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val sinceFilter = Random.shuffle(infos.map(_.eventDate)).head

      val pagedResults = eventsFinder
        .findEvents(Criteria(Filters.EventsSince(sinceFilter)))
        .unsafeRunSync()

      val expectedResult = infos.filter(_.eventDate.value.compareTo(sinceFilter.value) >= 0)
      pagedResults.pagingInfo.total.value   shouldBe expectedResult.size
      pagedResults.pagingInfo.pagingRequest shouldBe PagingRequest.default
      pagedResults.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
    }

    "return the List of events of all projects matching the given until filter" in new TestCase {

      val infos = eventInfos().generateList(min = 3, max = 10)

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, info.project.id)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.project.path,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val untilFilter = Random.shuffle(infos.map(_.eventDate)).head

      val pagedResults = eventsFinder
        .findEvents(Criteria(Filters.EventsUntil(untilFilter)))
        .unsafeRunSync()

      val expectedResult = infos.filter(_.eventDate.value.compareTo(untilFilter.value) <= 0)
      pagedResults.pagingInfo.total.value   shouldBe expectedResult.size
      pagedResults.pagingInfo.pagingRequest shouldBe PagingRequest.default
      pagedResults.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
    }

    "return the List of events of all projects matching the given since and until filters" in new TestCase {

      val infos = eventInfos().generateList(min = 3, max = 10)

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, info.project.id)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.project.path,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val filter = Random.shuffle(infos.map(_.eventDate)).head

      val pagedResults = eventsFinder
        .findEvents(Criteria(Filters.EventsSinceAndUntil(Filters.EventsSince(filter), Filters.EventsUntil(filter))))
        .unsafeRunSync()

      val expectedResult = infos.filter(_.eventDate == filter)
      pagedResults.pagingInfo.total.value   shouldBe expectedResult.size
      pagedResults.pagingInfo.pagingRequest shouldBe PagingRequest.default
      pagedResults.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
    }

    "return the List of events with the same id and status present in more than one project" in new TestCase {
      val id          = eventIds.generateOne
      val eventStatus = eventStatuses.generateOne
      val info1       = eventInfos().generateOne.copy(eventId = id, status = eventStatus)
      val info2       = eventInfos().generateOne.copy(eventId = id, status = eventStatus)

      List(info1, info2) foreach { info =>
        val eventId = CompoundEventId(info.eventId, info.project.id)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.project.path,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val pagingRequest = PagingRequest(Page(2), PerPage(1))

      val pagedResults = eventsFinder
        .findEvents(Criteria(Filters.EventsWithStatus(eventStatus, maybeDates = None), paging = pagingRequest))
        .unsafeRunSync()

      pagedResults.pagingInfo.total.value   shouldBe 2
      pagedResults.pagingInfo.pagingRequest shouldBe pagingRequest
      pagedResults.results                  shouldBe List(List(info1, info2).sortBy(_.eventDate).reverse.last)
    }

    "return an empty List if there's no project with the given path" in new TestCase {
      eventsFinder
        .findEvents(Criteria(Filters.ProjectEvents(projectPath, maybeStatus = None, maybeDates = None)))
        .unsafeRunSync()
        .results shouldBe Nil
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne

    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val eventsFinder     = new EventsFinderImpl[IO](queriesExecTimes)
  }
}
