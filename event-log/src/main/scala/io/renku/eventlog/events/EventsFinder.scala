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

import cats.data.Kleisli
import cats.effect.{BracketThrow, Concurrent, ConcurrentEffect, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlStatement}
import ch.datascience.graph.model.events.{EventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.http.rest.paging.Paging.PagedResultsFinder
import ch.datascience.http.rest.paging.model.{PerPage, Total}
import ch.datascience.http.rest.paging.{Paging, PagingRequest, PagingResponse}
import ch.datascience.metrics.LabeledHistogram
import io.renku.eventlog._
import io.renku.eventlog.events.EventsEndpoint.{EventInfo, Request, StatusProcessingTime}

private trait EventsFinder[Interpretation[_]] {
  def findEvents(request: EventsEndpoint.Request): Interpretation[PagingResponse[EventInfo]]
}

private class EventsFinderImpl[Interpretation[_]: BracketThrow: Concurrent](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name]
) extends DbClient[Interpretation](Some(queriesExecTimes))
    with EventsFinder[Interpretation]
    with Paging[EventInfo] {

  import ch.datascience.db.implicits._
  import eu.timepit.refined.auto._
  import skunk._
  import skunk.codec.numeric._
  import skunk.implicits._

  override def findEvents(request: EventsEndpoint.Request): Interpretation[PagingResponse[EventInfo]] = {
    implicit val finder: PagedResultsFinder[Interpretation, EventInfo] =
      createFinder(request)
    findPage(request.pagingRequest)
  }

  private def createFinder(request: EventsEndpoint.Request) =
    new PagedResultsFinder[Interpretation, EventInfo] with TypeSerializers {

      override def findResults(paging: PagingRequest): Interpretation[List[EventInfo]] =
        sessionResource.useK {
          for {
            infos               <- find()
            withProcessingTimes <- infos.map(addProcessingTimes()).sequence
          } yield withProcessingTimes
        }

      val selectEventInfo: Fragment[Void] =
        sql"""
            SELECT evt.event_id, prj.project_path, evt.status, evt.event_date, evt.execution_date, evt.message,  COUNT(times.status)
            FROM event evt
        """

      val joinProject: Fragment[Void] =
        sql"""
            JOIN project prj ON evt.project_id = prj.project_id
         """

      val filteByProject: Fragment[projects.Path] =
        sql"""AND prj.project_path = $projectPathEncoder """

      val joinProcessingTime: Fragment[Void] =
        sql"""
            LEFT JOIN status_processing_time times ON evt.event_id = times.event_id AND evt.project_id = times.project_id
          """

      val statusFilter: Fragment[EventStatus] =
        sql"""
           WHERE evt.status = $eventStatusEncoder
        """

      val groupAndPaginate: Fragment[Int ~ PerPage] =
        sql"""
              GROUP BY evt.event_id, evt.status, evt.event_date, evt.execution_date, evt.message, prj.project_path
              ORDER BY evt.event_date DESC, evt.event_id
              OFFSET $int4
              LIMIT $perPageEncoder
          """

      private def selectEventInfoQuery: EventsEndpoint.Request => AppliedFragment = {
        case Request.ProjectEvents(projectPath, None, pagingRequest) =>
          selectEventInfo(Void) |+|
            joinProject(Void) |+|
            filteByProject(projectPath) |+|
            joinProcessingTime(Void) |+|
            groupAndPaginate(
              ((pagingRequest.page.value - 1) * pagingRequest.perPage.value) ~ pagingRequest.perPage
            )
        case Request.ProjectEvents(projectPath, Some(status), pagingRequest) =>
          selectEventInfo(Void) |+|
            joinProject(Void) |+|
            filteByProject(projectPath) |+|
            joinProcessingTime(Void) |+|
            statusFilter(status) |+|
            groupAndPaginate(((pagingRequest.page.value - 1) * pagingRequest.perPage.value) ~ pagingRequest.perPage)
        case Request.EventsWithStatus(status, pagingRequest) =>
          selectEventInfo(Void) |+|
            joinProject(Void) |+|
            joinProcessingTime(Void) |+|
            statusFilter(status) |+|
            groupAndPaginate(((pagingRequest.page.value - 1) * pagingRequest.perPage.value) ~ pagingRequest.perPage)

      }

      private def find() = measureExecutionTime {

        val query: AppliedFragment = selectEventInfoQuery(request)
        SqlStatement[Interpretation](name = "find event infos")
          .select[query.A, (EventInfo, Long)](
            query.fragment
              .query(
                eventIdDecoder ~ projectPathDecoder ~ eventStatusDecoder ~ eventDateDecoder ~ executionDateDecoder ~ eventMessageDecoder.opt ~ int8
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

      private def addProcessingTimes()
          : ((EventInfo, Long)) => Kleisli[Interpretation, Session[Interpretation], EventInfo] = {
        case (info, 0L) => Kleisli.pure(info)
        case (info, _) =>
          findStatusProcessingTimes(info)
            .map(processingTimes => info.copy(processingTimes = processingTimes.sortBy(_.status)))
      }

      private def findStatusProcessingTimes(eventInfo: EventInfo) = measureExecutionTime {
        SqlStatement[Interpretation](name = "find event processing times")
          .select[EventId ~ projects.Path, StatusProcessingTime](
            sql"""SELECT times.status, times.processing_time
              FROM status_processing_time times
              WHERE times.event_id = $eventIdEncoder AND times.project_id = (
                SELECT project_id FROM project WHERE project_path = $projectPathEncoder
                ORDER BY project_id DESC
                LIMIT 1
              )
          """
              .query(statusProcessingTimesDecoder)
          )
          .arguments(eventInfo.eventId, eventInfo.projectPath)
          .build(_.toList)
      }

      private def countEventQuery: EventsEndpoint.Request => AppliedFragment = {
        case Request.ProjectEvents(projectPath, None, _) =>
          val query: Fragment[projects.Path] =
            sql"""
             SELECT COUNT(DISTINCT evt.event_id)
             FROM event evt
             JOIN project prj ON evt.project_id = prj.project_id AND prj.project_path = $projectPathEncoder
           """
          query(projectPath)
        case Request.ProjectEvents(projectPath, Some(status), _) =>
          val query: Fragment[projects.Path ~ EventStatus] =
            sql"""
             SELECT COUNT(DISTINCT evt.event_id)
             FROM event evt
             JOIN project prj ON evt.project_id = prj.project_id AND prj.project_path = $projectPathEncoder
              WHERE evt.status = $eventStatusEncoder
           """
          query(projectPath ~ status)
        case Request.EventsWithStatus(status, _) =>
          val query: Fragment[EventStatus] =
            sql"""
             SELECT COUNT(DISTINCT evt.event_id)
             FROM event evt
             JOIN project prj ON evt.project_id = prj.project_id
              WHERE evt.status = $eventStatusEncoder
           """
          query(status)

      }

      override def findTotal(): Interpretation[Total] = sessionResource.useK {
        val query = countEventQuery(request)
        measureExecutionTime {
          SqlStatement[Interpretation](name = "find event infos total")
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
  def apply(sessionResource:   SessionResource[IO, EventLogDB],
            queriesExecTimes:  LabeledHistogram[IO, SqlStatement.Name]
  )(implicit concurrentEffect: ConcurrentEffect[IO]): IO[EventsFinder[IO]] = IO(
    new EventsFinderImpl(sessionResource, queriesExecTimes)
  )
}
