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

import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel}
import io.renku.db.{DbClient, SessionResource, SqlStatement}
import io.renku.eventlog._
import io.renku.eventlog.events.EventsEndpoint.{Criteria, EventInfo, StatusProcessingTime}
import io.renku.graph.model.events.{EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.http.rest.paging.Paging.PagedResultsFinder
import io.renku.http.rest.paging.model.{PerPage, Total}
import io.renku.http.rest.paging.{Paging, PagingRequest, PagingResponse}
import io.renku.metrics.LabeledHistogram

private trait EventsFinder[F[_]] {
  def findEvents(request: EventsEndpoint.Criteria): F[PagingResponse[EventInfo]]
}

private class EventsFinderImpl[F[_]: Async: NonEmptyParallel](
    sessionResource:  SessionResource[F, EventLogDB],
    queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
) extends DbClient[F](Some(queriesExecTimes))
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

      override def findResults(paging: PagingRequest): F[List[EventInfo]] = sessionResource.useK {
        for {
          infos               <- findInfos()
          withProcessingTimes <- infos.map(addProcessingTimes()).sequence
        } yield withProcessingTimes
      }

      private val selectEventInfo: Fragment[Void] = sql"""
        SELECT evt.event_id, prj.project_path, evt.status, evt.event_date, evt.execution_date, evt.message,  COUNT(times.status)
        FROM event evt
      """

      private val joinProject: Fragment[Void] = sql"""
        JOIN project prj ON evt.project_id = prj.project_id
      """

      private val filterByProject: Fragment[projects.Path] = sql"""
        AND prj.project_path = $projectPathEncoder 
      """

      private val joinProcessingTime: Fragment[Void] = sql"""
        LEFT JOIN status_processing_time times ON evt.event_id = times.event_id AND evt.project_id = times.project_id
      """

      private val statusFilter: Fragment[EventStatus] = sql"""
        WHERE evt.status = $eventStatusEncoder
      """

      private val groupBy: Fragment[Void] = sql"""
        GROUP BY evt.event_id, evt.status, evt.event_date, evt.execution_date, evt.message, prj.project_path
      """

      private val orderBy: Criteria.Sorting.By => Fragment[Void] = {
        case Criteria.Sorting.By(Criteria.Sorting.EventDate, dir) => sql"""
          ORDER BY evt.event_date #${dir.name.toUpperCase}, evt.event_id
        """
      }

      private def paginate: Fragment[Int ~ PerPage] = sql"""
        OFFSET $int4
        LIMIT $perPageEncoder
      """

      private def selectEventInfoQuery: EventsEndpoint.Criteria => AppliedFragment = {
        case Criteria(Criteria.Filters.ProjectEvents(projectPath, None), sorting, paging) =>
          selectEventInfo(Void) |+|
            joinProject(Void) |+|
            filterByProject(projectPath) |+|
            joinProcessingTime(Void) |+|
            groupBy(Void) |+|
            orderBy(sorting)(Void) |+|
            paginate(((paging.page.value - 1) * paging.perPage.value) ~ paging.perPage)
        case Criteria(Criteria.Filters.ProjectEvents(projectPath, Some(status)), sorting, paging) =>
          selectEventInfo(Void) |+|
            joinProject(Void) |+|
            filterByProject(projectPath) |+|
            joinProcessingTime(Void) |+|
            statusFilter(status) |+|
            groupBy(Void) |+|
            orderBy(sorting)(Void) |+|
            paginate(((paging.page.value - 1) * paging.perPage.value) ~ paging.perPage)
        case Criteria(Criteria.Filters.EventsWithStatus(status), sorting, paging) =>
          selectEventInfo(Void) |+|
            joinProject(Void) |+|
            joinProcessingTime(Void) |+|
            statusFilter(status) |+|
            groupBy(Void) |+|
            orderBy(sorting)(Void) |+|
            paginate(((paging.page.value - 1) * paging.perPage.value) ~ paging.perPage)
      }

      private def findInfos() = measureExecutionTime {
        val query: AppliedFragment = selectEventInfoQuery(criteria)
        SqlStatement[F](name = "find event infos")
          .select[query.A, (EventInfo, Long)](
            query.fragment
              .query(
                eventIdDecoder ~ projectPathDecoder ~ eventStatusDecoder ~ eventDateDecoder ~
                  executionDateDecoder ~ eventMessageDecoder.opt ~ int8
              )
              .map {
                case (eventId:    EventId) ~
                    (projectPath: projects.Path) ~
                    (status: EventStatus) ~
                    (eventDate: EventDate) ~
                    (executionDate: ExecutionDate) ~
                    (maybeMessage: Option[EventMessage]) ~
                    (processingTimesCount: Long) =>
                  EventInfo(eventId,
                            projectPath,
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
          .select[EventId ~ projects.Path, StatusProcessingTime](
            sql"""SELECT times.status, times.processing_time
              FROM status_processing_time times
              WHERE times.event_id = $eventIdEncoder AND times.project_id = (
                SELECT project_id FROM project WHERE project_path = $projectPathEncoder
                ORDER BY project_id DESC
                LIMIT 1
              )
          """.query(statusProcessingTimesDecoder)
          )
          .arguments(eventInfo.eventId, eventInfo.projectPath)
          .build(_.toList)
      }

      private def countEventQuery: EventsEndpoint.Criteria => AppliedFragment = {
        case Criteria(Criteria.Filters.ProjectEvents(projectPath, None), _, _) =>
          val query: Fragment[projects.Path] = sql"""
             SELECT COUNT(DISTINCT evt.event_id)
             FROM event evt
             JOIN project prj ON evt.project_id = prj.project_id AND prj.project_path = $projectPathEncoder
           """
          query(projectPath)
        case Criteria(Criteria.Filters.ProjectEvents(projectPath, Some(status)), _, _) =>
          val query: Fragment[projects.Path ~ EventStatus] = sql"""
             SELECT COUNT(DISTINCT evt.event_id)
             FROM event evt
             JOIN project prj ON evt.project_id = prj.project_id AND prj.project_path = $projectPathEncoder
             WHERE evt.status = $eventStatusEncoder
           """
          query(projectPath ~ status)
        case Criteria(Criteria.Filters.EventsWithStatus(status), _, _) =>
          val query: Fragment[EventStatus] = sql"""
             SELECT COUNT(*)
             FROM (
                SELECT evt.event_id, evt.project_id FROM event evt
                WHERE evt.status = $eventStatusEncoder
                GROUP BY evt.event_id, evt.project_id
             ) event
           """
          query(status)
      }

      override def findTotal(): F[Total] = sessionResource.useK {
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
  def apply[F[_]: Async: NonEmptyParallel](sessionResource: SessionResource[F, EventLogDB],
                                           queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ): F[EventsFinder[F]] = MonadThrow[F].catchNonFatal(
    new EventsFinderImpl(sessionResource, queriesExecTimes)
  )
}
