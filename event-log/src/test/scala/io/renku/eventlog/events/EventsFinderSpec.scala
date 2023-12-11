/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.fixed
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectSlugs}
import io.renku.graph.model.events.{CompoundEventId, EventInfo}
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.rest.{SortBy, Sorting}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import scala.util.Random

class EventsFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {

  private val projectSlug = projectSlugs.generateOne

  it should "return the List of events of the project the given slug or id" in testDBResource.use { implicit cfg =>
    val projectId = projectIds.generateOne
    val infos     = eventInfos(fixed(projectSlug), fixed(projectId)).generateList(max = 10)
    for {
      _ <- infos.traverse_(storeEventFrom)

      _ <- storeGeneratedEvent(eventStatuses.generateOne,
                               eventDates.generateOne,
                               projectIds.generateOne,
                               projectSlugs.generateOne
           )

      _ <- eventsFinder
             .findEvents(Criteria(Filters.ProjectEvents(projectSlug, maybeStatus = None, maybeDates = None)))
             .asserting { results =>
               results.pagingInfo.total.value   shouldBe infos.size
               results.pagingInfo.pagingRequest shouldBe PagingRequest.default
               results.results                  shouldBe infos.sortBy(_.eventDate).reverse
             }

      _ <- eventsFinder
             .findEvents(Criteria(Filters.ProjectEvents(projectId, maybeStatus = None, maybeDates = None)))
             .asserting { results =>
               results.pagingInfo.total.value   shouldBe infos.size
               results.pagingInfo.pagingRequest shouldBe PagingRequest.default
               results.results                  shouldBe infos.sortBy(_.eventDate).reverse
             }
    } yield Succeeded
  }

  it should "return the List of events of the project with the given slug and the given PagingRequest" in testDBResource
    .use { implicit cfg =>
      val projectId = projectIds.generateOne
      val infos     = eventInfos(fixed(projectSlug), fixed(projectId)).generateList(min = 3, max = 10)
      for {
        _ <- infos.traverse_(storeEventFrom)

        pagingRequest: PagingRequest = PagingRequest(Page(2), PerPage(1))
        _ <- eventsFinder
               .findEvents(
                 Criteria(Filters.ProjectEvents(projectSlug, maybeStatus = None, maybeDates = None),
                          paging = pagingRequest
                 )
               )
               .asserting { results =>
                 results.pagingInfo.total.value   shouldBe infos.size
                 results.pagingInfo.pagingRequest shouldBe pagingRequest
                 results.results                  shouldBe List(infos.sortBy(_.eventDate).reverse.tail.head)
               }
      } yield Succeeded
    }

  it should "return the List of events of the project with the given slug and the given direction" in testDBResource
    .use { implicit cfg =>
      val projectId = projectIds.generateOne
      val infos     = eventInfos(fixed(projectSlug), fixed(projectId)).generateList(min = 3, max = 10)
      for {
        _ <- infos.traverse_(storeEventFrom)

        pagingRequest = PagingRequest(Page(1), PerPage(3))
        _ <- eventsFinder
               .findEvents(
                 Criteria(Filters.ProjectEvents(projectSlug, maybeStatus = None, maybeDates = None),
                          Sorting(Sort.By(Sort.EventDate, SortBy.Direction.Asc)),
                          pagingRequest
                 )
               )
               .asserting { results =>
                 results.pagingInfo.total.value   shouldBe infos.size
                 results.pagingInfo.pagingRequest shouldBe pagingRequest
                 results.results                  shouldBe infos.sortBy(_.eventDate).take(3)
               }

        _ <- eventsFinder
               .findEvents(
                 Criteria(
                   Filters.ProjectEvents(projectSlug, maybeStatus = None, maybeDates = None),
                   Sorting(Sort.By(Sort.EventDate, SortBy.Direction.Desc)),
                   pagingRequest
                 )
               )
               .asserting { results =>
                 results.pagingInfo.total.value   shouldBe infos.size
                 results.pagingInfo.pagingRequest shouldBe pagingRequest
                 results.results                  shouldBe infos.sortBy(_.eventDate).reverse.take(3)
               }
      } yield Succeeded
    }

  it should "return the List of events of the project with the given slug and status filter" in testDBResource.use {
    implicit cfg =>
      val projectId = projectIds.generateOne
      val infos     = eventInfos(fixed(projectSlug), fixed(projectId)).generateList(min = 3, max = 10)
      for {
        _ <- infos.traverse_(storeEventFrom)

        eventStatusFilter = Random.shuffle(infos.map(_.status)).head
        _ <- eventsFinder
               .findEvents(Criteria(Filters.ProjectEvents(projectSlug, eventStatusFilter.some, maybeDates = None)))
               .asserting { results =>
                 val expectedResult = infos.filter(_.status == eventStatusFilter)
                 results.pagingInfo.total.value   shouldBe expectedResult.size
                 results.pagingInfo.pagingRequest shouldBe PagingRequest.default
                 results.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
               }
      } yield Succeeded
  }

  it should "return the List of events of the project with the given slug and since filters" in testDBResource.use {
    implicit cfg =>
      val projectId = projectIds.generateOne
      val infos     = eventInfos(fixed(projectSlug), fixed(projectId)).generateList(min = 3, max = 10)
      for {
        _ <- infos.traverse_(storeEventFrom)

        sinceFilter = Random.shuffle(infos.map(_.eventDate)).head
        _ <- eventsFinder
               .findEvents(
                 Criteria(
                   Filters.ProjectEvents(projectSlug, maybeStatus = None, Filters.EventsSince(sinceFilter).some)
                 )
               )
               .asserting { results =>
                 val expectedResult = infos.filter(_.eventDate.value.compareTo(sinceFilter.value) >= 0)
                 results.pagingInfo.total.value   shouldBe expectedResult.size
                 results.pagingInfo.pagingRequest shouldBe PagingRequest.default
                 results.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
               }
      } yield Succeeded
  }

  it should "return the List of events of the project with the given slug and until filters" in testDBResource.use {
    implicit cfg =>
      val projectId = projectIds.generateOne
      val infos     = eventInfos(fixed(projectSlug), fixed(projectId)).generateList(min = 3, max = 10)
      for {
        _ <- infos.traverse_(storeEventFrom)

        untilFilter = Random.shuffle(infos.map(_.eventDate)).head
        _ <- eventsFinder
               .findEvents(
                 Criteria(
                   Filters.ProjectEvents(projectSlug, maybeStatus = None, Filters.EventsUntil(untilFilter).some)
                 )
               )
               .asserting { results =>
                 val expectedResult = infos.filter(_.eventDate.value.compareTo(untilFilter.value) <= 0)
                 results.pagingInfo.total.value   shouldBe expectedResult.size
                 results.pagingInfo.pagingRequest shouldBe PagingRequest.default
                 results.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
               }
      } yield Succeeded
  }

  it should "return the List of events of all projects matching the given status filter" in testDBResource.use {
    implicit cfg =>
      val infos = eventInfos().generateList(min = 3, max = 10)
      for {
        _ <- infos.traverse_(storeEventFrom)

        eventStatusFilter = Random.shuffle(infos.map(_.status)).head
        _ <- eventsFinder
               .findEvents(Criteria(Filters.EventsWithStatus(eventStatusFilter, maybeDates = None)))
               .asserting { results =>
                 val expectedResult = infos.filter(_.status == eventStatusFilter)
                 results.pagingInfo.total.value   shouldBe expectedResult.size
                 results.pagingInfo.pagingRequest shouldBe PagingRequest.default
                 results.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
               }
      } yield Succeeded
  }

  it should "return the List of events of all projects matching the given status and since filters" in testDBResource
    .use { implicit cfg =>
      val status = eventStatuses.generateOne
      val infos  = eventInfos().generateFixedSizeList(ofSize = 3).map(_.copy(status = status))
      for {
        _ <- infos.traverse_(storeEventFrom)

        _ :: mid :: youngest :: Nil = infos.sortBy(_.eventDate.value)
        _ <- eventsFinder
               .findEvents(Criteria(Filters.EventsWithStatus(status, Filters.EventsSince(mid.eventDate).some)))
               .asserting { results =>
                 val expectedResult = List(mid, youngest)
                 results.pagingInfo.total.value   shouldBe expectedResult.size
                 results.pagingInfo.pagingRequest shouldBe PagingRequest.default
                 results.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
               }
      } yield Succeeded
    }

  it should "return the List of events of all projects matching the given status and until filters" in testDBResource
    .use { implicit cfg =>
      val status = eventStatuses.generateOne
      val infos  = eventInfos().generateFixedSizeList(ofSize = 3).map(_.copy(status = status))
      for {
        _ <- infos.traverse_(storeEventFrom)

        oldest :: mid :: _ :: Nil = infos.sortBy(_.eventDate.value)
        _ <- eventsFinder
               .findEvents(Criteria(Filters.EventsWithStatus(status, Filters.EventsUntil(mid.eventDate).some)))
               .asserting { results =>
                 val expectedResult = List(oldest, mid)
                 results.pagingInfo.total.value   shouldBe expectedResult.size
                 results.pagingInfo.pagingRequest shouldBe PagingRequest.default
                 results.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
               }
      } yield Succeeded
    }

  it should "return the List of events of all projects matching the given since filter" in testDBResource.use {
    implicit cfg =>
      val infos = eventInfos().generateList(min = 3, max = 10)
      for {
        _ <- infos.traverse_(storeEventFrom)

        sinceFilter = Random.shuffle(infos.map(_.eventDate)).head
        _ <- eventsFinder
               .findEvents(Criteria(Filters.EventsSince(sinceFilter)))
               .asserting { results =>
                 val expectedResult = infos.filter(_.eventDate.value.compareTo(sinceFilter.value) >= 0)
                 results.pagingInfo.total.value   shouldBe expectedResult.size
                 results.pagingInfo.pagingRequest shouldBe PagingRequest.default
                 results.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
               }
      } yield Succeeded
  }

  it should "return the List of events of all projects matching the given until filter" in testDBResource.use {
    implicit cfg =>
      val infos = eventInfos().generateList(min = 3, max = 10)
      for {
        _ <- infos.traverse_(storeEventFrom)

        untilFilter = Random.shuffle(infos.map(_.eventDate)).head
        _ <- eventsFinder
               .findEvents(Criteria(Filters.EventsUntil(untilFilter)))
               .asserting { results =>
                 val expectedResult = infos.filter(_.eventDate.value.compareTo(untilFilter.value) <= 0)
                 results.pagingInfo.total.value   shouldBe expectedResult.size
                 results.pagingInfo.pagingRequest shouldBe PagingRequest.default
                 results.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
               }
      } yield Succeeded
  }

  it should "return the List of events of all projects matching the given since and until filters" in testDBResource
    .use { implicit cfg =>
      val infos = eventInfos().generateList(min = 3, max = 10)
      for {
        _ <- infos.traverse_(storeEventFrom)

        filter = Random.shuffle(infos.map(_.eventDate)).head
        _ <-
          eventsFinder
            .findEvents(Criteria(Filters.EventsSinceAndUntil(Filters.EventsSince(filter), Filters.EventsUntil(filter))))
            .asserting { results =>
              val expectedResult = infos.filter(_.eventDate == filter)
              results.pagingInfo.total.value   shouldBe expectedResult.size
              results.pagingInfo.pagingRequest shouldBe PagingRequest.default
              results.results                  shouldBe expectedResult.sortBy(_.eventDate).reverse
            }
      } yield Succeeded
    }

  it should "return the List of events with the same id and status present in more than one project" in testDBResource
    .use { implicit cfg =>
      val id          = eventIds.generateOne
      val eventStatus = eventStatuses.generateOne
      val info1       = eventInfos().generateOne.copy(eventId = id, status = eventStatus)
      val info2       = eventInfos().generateOne.copy(eventId = id, status = eventStatus)
      for {
        _ <- List(info1, info2).traverse_(storeEventFrom)

        pagingRequest = PagingRequest(Page(2), PerPage(1))
        _ <- eventsFinder
               .findEvents(Criteria(Filters.EventsWithStatus(eventStatus, maybeDates = None), paging = pagingRequest))
               .asserting { results =>
                 results.pagingInfo.total.value   shouldBe 2
                 results.pagingInfo.pagingRequest shouldBe pagingRequest
                 results.results                  shouldBe List(List(info1, info2).sortBy(_.eventDate).reverse.last)
               }
      } yield Succeeded

    }

  it should "return an empty List if there's no project with the given slug" in testDBResource.use { implicit cfg =>
    eventsFinder
      .findEvents(Criteria(Filters.ProjectEvents(projectSlug, maybeStatus = None, maybeDates = None)))
      .asserting(_.results shouldBe Nil)
  }

  private def eventsFinder(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new EventsFinderImpl[IO]
  }

  private def storeEventFrom(info: EventInfo)(implicit cfg: DBConfig[EventLogDB]) = {
    val eventId = CompoundEventId(info.eventId, info.project.id)
    storeEvent(eventId,
               info.status,
               info.executionDate,
               info.eventDate,
               eventBodies.generateOne,
               projectSlug = info.project.slug,
               maybeMessage = info.maybeMessage
    ) >>
      info.processingTimes
        .map(pt => upsertProcessingTime(eventId, pt.status, pt.processingTime))
        .sequence
  }
}
