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
import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.eventlog.events.Generators._
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
      val infos     = eventInfos(fixed(projectPath)).generateList(maxElements = 10)

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, projectId)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.projectPath,
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
        .findEvents(Criteria(Filters.ProjectEvents(projectPath, None)))
        .unsafeRunSync()

      pagedResults.pagingInfo.total.value   shouldBe infos.size
      pagedResults.pagingInfo.pagingRequest shouldBe PagingRequest.default
      pagedResults.results                  shouldBe infos.sortBy(_.eventDate).reverse
    }

    "return the List of events of the project with the given path and the given PagingRequest" in new TestCase {
      val projectId = projectIds.generateOne
      val infos     = eventInfos(fixed(projectPath)).generateList(minElements = 3, maxElements = 10)

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, projectId)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.projectPath,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val pagingRequest: PagingRequest = PagingRequest(Page(2), PerPage(1))
      val pagedResults = eventsFinder
        .findEvents(Criteria(Filters.ProjectEvents(projectPath, None), paging = pagingRequest))
        .unsafeRunSync()

      pagedResults.pagingInfo.total.value   shouldBe infos.size
      pagedResults.pagingInfo.pagingRequest shouldBe pagingRequest
      pagedResults.results                  shouldBe List(infos.sortBy(_.eventDate).reverse.tail.head)
    }

    "return the List of events of the project with the given path and the given direction" in new TestCase {
      val projectId = projectIds.generateOne
      val infos     = eventInfos(fixed(projectPath)).generateList(minElements = 3, maxElements = 10)

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, projectId)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.projectPath,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val pagingRequest = PagingRequest(Page(1), PerPage(3))
      val pagedResults = eventsFinder
        .findEvents(
          Criteria(Filters.ProjectEvents(projectPath, None),
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
          Criteria(Filters.ProjectEvents(projectPath, None),
                   Sorting.By(Sorting.EventDate, SortBy.Direction.Desc),
                   pagingRequest
          )
        )
        .unsafeRunSync()

      ascPagedResults.pagingInfo.total.value   shouldBe infos.size
      ascPagedResults.pagingInfo.pagingRequest shouldBe pagingRequest
      ascPagedResults.results                  shouldBe infos.sortBy(_.eventDate).reverse.take(3)
    }

    "return the List of events of the project with the given path and the given the status filter" in new TestCase {
      val projectId = projectIds.generateOne
      val infos     = eventInfos(fixed(projectPath)).generateList(minElements = 3, maxElements = 10)

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, projectId)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.projectPath,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val eventStatusFilter = Random.shuffle(infos.map(_.status)).head
      val pagedResults = eventsFinder
        .findEvents(Criteria(Filters.ProjectEvents(projectPath, eventStatusFilter.some)))
        .unsafeRunSync()

      val expectedResult = infos.filter(_.status == eventStatusFilter)
      pagedResults.pagingInfo.total.value   shouldBe expectedResult.size
      pagedResults.pagingInfo.pagingRequest shouldBe PagingRequest.default
      pagedResults.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
    }

    "return the List of events of all projects matching the given the status filter" in new TestCase {

      val infos = eventInfos().generateList(minElements = 3, maxElements = 10)

      infos foreach { info =>
        val eventId = CompoundEventId(info.eventId, projectIds.generateOne)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.projectPath,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val eventStatusFilter = Random.shuffle(infos.map(_.status)).head

      val expectedResult = infos.filter(_.status == eventStatusFilter)

      val pagedResults = eventsFinder
        .findEvents(Criteria(Filters.EventsWithStatus(eventStatusFilter)))
        .unsafeRunSync()

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
        val eventId = CompoundEventId(info.eventId, projectIds.generateOne)
        storeEvent(
          eventId,
          info.status,
          info.executionDate,
          info.eventDate,
          eventBodies.generateOne,
          projectPath = info.projectPath,
          maybeMessage = info.maybeMessage
        )
        info.processingTimes.foreach(processingTime =>
          upsertProcessingTime(eventId, processingTime.status, processingTime.processingTime)
        )
      }

      val pagingRequest = PagingRequest(Page(2), PerPage(1))

      val pagedResults = eventsFinder
        .findEvents(Criteria(Filters.EventsWithStatus(eventStatus), paging = pagingRequest))
        .unsafeRunSync()

      pagedResults.pagingInfo.total.value   shouldBe 2
      pagedResults.pagingInfo.pagingRequest shouldBe pagingRequest
      pagedResults.results                  shouldBe List(List(info1, info2).sortBy(_.eventDate).reverse.last)
    }

    "return an empty List if there's no project with the given path" in new TestCase {
      eventsFinder
        .findEvents(Criteria(Filters.ProjectEvents(projectPath, None)))
        .unsafeRunSync()
        .results shouldBe Nil
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne

    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val eventsFinder     = new EventsFinderImpl[IO](sessionResource, queriesExecTimes)
  }
}
