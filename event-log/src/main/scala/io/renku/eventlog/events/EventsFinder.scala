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

import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel}
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog._
import io.renku.eventlog.events.EventsEndpoint.Criteria
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.model.events._
import io.renku.graph.model.projects
import io.renku.http.rest.Sorting
import io.renku.http.rest.paging.Paging.PagedResultsFinder
import io.renku.http.rest.paging.model.{PerPage, Total}
import io.renku.http.rest.paging.{Paging, PagingRequest, PagingResponse}

private trait EventsFinder[F[_]] {
  def findEvents(request: EventsEndpoint.Criteria): F[PagingResponse[EventInfo]]
}

private class EventsFinderImpl[F[_]: Async: NonEmptyParallel: SessionResource: QueriesExecutionTimes]
    extends DbClient[F](Some(QueriesExecutionTimes[F]))
    with EventsFinder[F]
    with Paging[EventInfo] {

  import eu.timepit.refined.auto._
  import io.renku.db.implicits._
  import skunk._
  import skunk.codec.numeric._
  import skunk.implicits._

  override def findEvents(criteria: EventsEndpoint.Criteria): F[PagingResponse[EventInfo]] = {
    implicit val finder: PagedResultsFinder[F, EventInfo] = createFinder(criteria)
    findPage(criteria.paging)
  }

  private def createFinder(criteria: EventsEndpoint.Criteria) =
    new PagedResultsFinder[F, EventInfo] with TypeSerializers {

      override def findResults(paging: PagingRequest): F[List[EventInfo]] = SessionResource[F].useK {
        for {
          infos               <- findInfos()
          withProcessingTimes <- infos.map(addProcessingTimes()).sequence
        } yield withProcessingTimes
      }

      private val selectEventInfo: Fragment[Void] = sql"""
        SELECT evt.event_id, prj.project_id, prj.project_path, evt.status, evt.event_date, evt.execution_date, evt.message,  COUNT(times.status)
        FROM event evt
      """

      private val joinProject: Fragment[Void] = sql"""
        JOIN project prj ON evt.project_id = prj.project_id
      """

      private val filterByProject: projects.Identifier => AppliedFragment = {
        case slug: projects.Slug =>
          val fragment: Fragment[projects.Slug] = sql"""AND prj.project_path = $projectSlugEncoder"""
          fragment(slug)
        case id: projects.GitLabId =>
          val fragment: Fragment[projects.GitLabId] = sql"""AND prj.project_id = $projectIdEncoder"""
          fragment(id)
      }

      private val whereEventDate: Option[Criteria.FiltersOnDate] => AppliedFragment = {
        case Some(Criteria.Filters.EventsSince(since)) =>
          val fragment: Fragment[EventDate] = sql"""
            WHERE evt.event_date >= $eventDateEncoder
          """
          fragment(since)
        case Some(Criteria.Filters.EventsUntil(until)) =>
          val fragment: Fragment[EventDate] = sql"""
            WHERE evt.event_date <= $eventDateEncoder
          """
          fragment(until)
        case Some(Criteria.Filters.EventsSinceAndUntil(since, until)) =>
          val fragment: Fragment[EventDate *: EventDate *: EmptyTuple] = sql"""
            WHERE evt.event_date >= $eventDateEncoder
              AND evt.event_date <= $eventDateEncoder
          """
          fragment(since.eventDate *: until.eventDate *: EmptyTuple)
        case None => AppliedFragment.empty
      }

      private val andEventDate: Option[Criteria.FiltersOnDate] => AppliedFragment = {
        case Some(Criteria.Filters.EventsSince(since)) =>
          val fragment: Fragment[EventDate] = sql"""
            AND evt.event_date >= $eventDateEncoder
          """
          fragment(since)
        case Some(Criteria.Filters.EventsUntil(until)) =>
          val fragment: Fragment[EventDate] = sql"""
            AND evt.event_date <= $eventDateEncoder
          """
          fragment(until)
        case Some(Criteria.Filters.EventsSinceAndUntil(since, until)) =>
          val fragment: Fragment[EventDate *: EventDate *: EmptyTuple] = sql"""
            AND evt.event_date >= $eventDateEncoder
            AND evt.event_date <= $eventDateEncoder
          """
          fragment(since.eventDate *: until.eventDate *: EmptyTuple)
        case None => AppliedFragment.empty
      }

      private val joinProcessingTime: Fragment[Void] = sql"""
        LEFT JOIN status_processing_time times ON evt.event_id = times.event_id AND evt.project_id = times.project_id
      """

      private val whereStatus: Fragment[EventStatus] = sql"""
        WHERE evt.status = $eventStatusEncoder
      """

      private val groupBy: Fragment[Void] = sql"""
        GROUP BY evt.event_id, evt.status, evt.event_date, evt.execution_date, evt.message, prj.project_id, prj.project_path
      """

      private val orderBy: Sorting[Criteria.Sort.type] => Fragment[Void] = sorting => {
        val sortSql = sorting.sortBy.map { case Criteria.Sort.By(Criteria.Sort.EventDate, dir) =>
          sql"evt.event_date #${dir.name.toUpperCase}"
        } :+ sql"evt.event_id"

        sql" ORDER BY " ~> sortSql.reduceLeft(_ ~> sql", " ~> _)
      }

      private def paginate: Fragment[Int *: PerPage *: EmptyTuple] = sql"""
        OFFSET $int4
        LIMIT $perPageEncoder
      """

      private def selectEventInfoQuery: EventsEndpoint.Criteria => AppliedFragment = {
        case Criteria(Criteria.Filters.ProjectEvents(projectId, None, maybeDates), sorting, paging) =>
          selectEventInfo(Void) |+|
            joinProject(Void) |+|
            filterByProject(projectId) |+|
            joinProcessingTime(Void) |+|
            whereEventDate(maybeDates) |+|
            groupBy(Void) |+|
            orderBy(sorting)(Void) |+|
            paginate(((paging.page.value - 1) * paging.perPage.value) *: paging.perPage *: EmptyTuple)
        case Criteria(Criteria.Filters.ProjectEvents(projectSlug, Some(status), maybeDates), sorting, paging) =>
          selectEventInfo(Void) |+|
            joinProject(Void) |+|
            filterByProject(projectSlug) |+|
            joinProcessingTime(Void) |+|
            whereStatus(status) |+|
            andEventDate(maybeDates) |+|
            groupBy(Void) |+|
            orderBy(sorting)(Void) |+|
            paginate(((paging.page.value - 1) * paging.perPage.value) *: paging.perPage *: EmptyTuple)
        case Criteria(Criteria.Filters.EventsWithStatus(status, maybeDates), sorting, paging) =>
          selectEventInfo(Void) |+|
            joinProject(Void) |+|
            joinProcessingTime(Void) |+|
            whereStatus(status) |+|
            andEventDate(maybeDates) |+|
            groupBy(Void) |+|
            orderBy(sorting)(Void) |+|
            paginate(((paging.page.value - 1) * paging.perPage.value) *: paging.perPage *: EmptyTuple)
        case Criteria(dates: Criteria.FiltersOnDate, sorting, paging) =>
          selectEventInfo(Void) |+|
            joinProject(Void) |+|
            joinProcessingTime(Void) |+|
            whereEventDate(dates.some) |+|
            groupBy(Void) |+|
            orderBy(sorting)(Void) |+|
            paginate(((paging.page.value - 1) * paging.perPage.value) *: paging.perPage *: EmptyTuple)
      }

      private def findInfos() = measureExecutionTime {
        val query: AppliedFragment = selectEventInfoQuery(criteria)
        SqlStatement[F](name = "find event infos")
          .select[query.A, (EventInfo, Long)](
            query.fragment
              .query(
                eventIdDecoder ~ projectIdsDecoder ~ eventStatusDecoder ~ eventDateDecoder ~
                  executionDateDecoder ~ eventMessageDecoder.opt ~ int8
              )
              .map {
                case (eventId:   EventId) ~
                    (projectIds: EventInfo.ProjectIds) ~
                    (status: EventStatus) ~
                    (eventDate: EventDate) ~
                    (executionDate: ExecutionDate) ~
                    (maybeMessage: Option[EventMessage]) ~
                    (processingTimesCount: Long) =>
                  EventInfo(eventId,
                            projectIds,
                            status,
                            eventDate,
                            executionDate,
                            maybeMessage,
                            processingTimes = List.empty
                  ) -> processingTimesCount
              }
          )
          .arguments(query.argument)
          .build(_.toList)
      }

      private def addProcessingTimes(): ((EventInfo, Long)) => Kleisli[F, Session[F], EventInfo] = {
        case (info, 0L) => Kleisli.pure(info)
        case (info, _) =>
          findStatusProcessingTimes(info)
            .map(processingTimes => info.copy(processingTimes = processingTimes.sortBy(_.status)))
      }

      private def findStatusProcessingTimes(eventInfo: EventInfo) = measureExecutionTime {
        SqlStatement[F](name = "find event processing times")
          .select[EventId *: projects.Slug *: EmptyTuple, StatusProcessingTime](
            sql"""SELECT times.status, times.processing_time
              FROM status_processing_time times
              WHERE times.event_id = $eventIdEncoder AND times.project_id = (
                SELECT project_id FROM project WHERE project_path = $projectSlugEncoder
                ORDER BY project_id DESC
                LIMIT 1
              )
          """.query(statusProcessingTimesDecoder)
          )
          .arguments(eventInfo.eventId *: eventInfo.project.slug *: EmptyTuple)
          .build(_.toList)
      }

      private def countEventQuery: EventsEndpoint.Criteria => AppliedFragment = {
        case Criteria(Criteria.Filters.ProjectEvents(projectSlug: projects.Slug, None, maybeDates), _, _) =>
          val query: Fragment[projects.Slug] = sql"""
             SELECT COUNT(DISTINCT evt.event_id)
             FROM event evt
             JOIN project prj ON evt.project_id = prj.project_id AND prj.project_path = $projectSlugEncoder
           """
          query(projectSlug) |+| whereEventDate(maybeDates)
        case Criteria(Criteria.Filters.ProjectEvents(projectId: projects.GitLabId, None, maybeDates), _, _) =>
          val query: Fragment[projects.GitLabId] = sql"""
             SELECT COUNT(DISTINCT evt.event_id)
             FROM event evt
             WHERE evt.project_id = $projectIdEncoder
           """
          query(projectId) |+| whereEventDate(maybeDates)
        case Criteria(Criteria.Filters.ProjectEvents(projectSlug: projects.Slug, Some(status), maybeDates), _, _) =>
          val query: Fragment[projects.Slug *: EventStatus *: EmptyTuple] = sql"""
             SELECT COUNT(DISTINCT evt.event_id)
             FROM event evt
             JOIN project prj ON evt.project_id = prj.project_id AND prj.project_path = $projectSlugEncoder
             WHERE evt.status = $eventStatusEncoder
           """
          query(projectSlug *: status *: EmptyTuple) |+| andEventDate(maybeDates)
        case Criteria(Criteria.Filters.ProjectEvents(projectId: projects.GitLabId, Some(status), maybeDates), _, _) =>
          val query: Fragment[projects.GitLabId *: EventStatus *: EmptyTuple] = sql"""
             SELECT COUNT(DISTINCT evt.event_id)
             FROM event evt
             WHERE evt.project_id = $projectIdEncoder AND evt.status = $eventStatusEncoder
           """
          query(projectId *: status *: EmptyTuple) |+| andEventDate(maybeDates)
        case Criteria(Criteria.Filters.EventsWithStatus(status, maybeDates), _, _) =>
          val query: Fragment[EventStatus] = sql"""
             SELECT COUNT(DISTINCT(evt.event_id, evt.project_id)) 
             FROM event evt
             WHERE evt.status = $eventStatusEncoder
           """
          query(status) |+| andEventDate(maybeDates)
        case Criteria(dates: Criteria.FiltersOnDate, _, _) =>
          val query: Fragment[Void] = sql"""
             SELECT COUNT(DISTINCT(evt.event_id, evt.project_id)) 
             FROM event evt
           """
          query(Void) |+| whereEventDate(dates.some)
      }

      override def findTotal(): F[Total] = SessionResource[F].useK {
        val query = countEventQuery(criteria)
        measureExecutionTime {
          SqlStatement[F](name = "find event infos total")
            .select[query.A, Total](query.fragment.query(int8).map((total: Long) => Total(total.toInt)))
            .arguments(query.argument)
            .build(_.option)
            .mapResult(_.getOrElse(Total(0)))
        }
      }

      private lazy val statusProcessingTimesDecoder: Decoder[StatusProcessingTime] =
        (eventStatusDecoder ~ eventProcessingTimeDecoder).map((StatusProcessingTime.apply _).tupled.apply)
    }
}

private object EventsFinder {
  def apply[F[_]: Async: NonEmptyParallel: SessionResource: QueriesExecutionTimes]: F[EventsFinder[F]] =
    MonadThrow[F].catchNonFatal(new EventsFinderImpl[F])
}
