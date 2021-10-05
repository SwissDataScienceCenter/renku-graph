/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.fixed
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.http.rest.paging.PagingRequest
import ch.datascience.http.rest.paging.model.{Page, PerPage}
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.eventlog.events.Generators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class EventsFinderSpec extends AnyWordSpec with InMemoryEventLogDbSpec with should.Matchers {

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
        .findEvents(EventsEndpoint.Request.ProjectEvents(projectPath, None, PagingRequest.default))
        .unsafeRunSync()

      pagedResults.pagingInfo.total.value   shouldBe infos.size
      pagedResults.pagingInfo.pagingRequest shouldBe PagingRequest.default
      pagedResults.results                  shouldBe infos.sortBy(_.eventDate).reverse
    }

    "return the List of events of the project the given path and the given PagingRequest" in new TestCase {
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
      val pagedResults =
        eventsFinder.findEvents(EventsEndpoint.Request.ProjectEvents(projectPath, None, pagingRequest)).unsafeRunSync()

      pagedResults.pagingInfo.total.value   shouldBe infos.size
      pagedResults.pagingInfo.pagingRequest shouldBe pagingRequest
      pagedResults.results                  shouldBe List(infos.sortBy(_.eventDate).reverse.tail.head)
    }

    "return the List of events of the project the given path and the given the status filter" in new TestCase {
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

      val expectedResult = infos.filter(_.status == eventStatusFilter)

      val pagedResults = eventsFinder
        .findEvents(EventsEndpoint.Request.ProjectEvents(projectPath, eventStatusFilter.some, PagingRequest.default))
        .unsafeRunSync()

      pagedResults.pagingInfo.total.value   shouldBe expectedResult.size
      pagedResults.pagingInfo.pagingRequest shouldBe PagingRequest.default
      pagedResults.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
    }

    "return the List of events of  all the projects and the given the status filter" in new TestCase {

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
        .findEvents(EventsEndpoint.Request.EventsWithStatus(eventStatusFilter, PagingRequest.default))
        .unsafeRunSync()

      pagedResults.pagingInfo.total.value   shouldBe expectedResult.size
      pagedResults.pagingInfo.pagingRequest shouldBe PagingRequest.default
      pagedResults.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
    }

    "return the List of events with the same id present on more than one project and the given the status filter" in new TestCase {
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

      val pagingRequest: PagingRequest = PagingRequest(Page(2), PerPage(1))

      val pagedResults = eventsFinder
        .findEvents(EventsEndpoint.Request.EventsWithStatus(eventStatus, pagingRequest))
        .unsafeRunSync()

      pagedResults.pagingInfo.total.value   shouldBe 2
      pagedResults.pagingInfo.pagingRequest shouldBe pagingRequest
      pagedResults.results                  shouldBe List(List(info1, info2).sortBy(_.eventDate).reverse.last)
    }

    "return an empty List if there's no project with the given path" in new TestCase {
      eventsFinder
        .findEvents(EventsEndpoint.Request.ProjectEvents(projectPath, None, PagingRequest.default))
        .unsafeRunSync()
        .results shouldBe Nil
    }

    "return an empty List if there are no events for the project with the given path" in new TestCase {
      upsertProject(projectIds.generateOne, projectPaths.generateOne, eventDates.generateOne)
      eventsFinder
        .findEvents(EventsEndpoint.Request.ProjectEvents(projectPath, None, PagingRequest.default))
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
