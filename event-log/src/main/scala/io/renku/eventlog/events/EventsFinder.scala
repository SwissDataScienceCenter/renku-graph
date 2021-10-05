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
import io.renku.eventlog.events.EventsEndpoint.{EventInfo, StatusProcessingTime}

private trait EventsFinder[Interpretation[_]] {
  def findEvents(projectPath:       projects.Path,
                 maybeStatusFilter: Option[EventStatus],
                 pagingRequest:     PagingRequest
  ): Interpretation[PagingResponse[EventInfo]]
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
  import skunk.AppliedFragment._

  override def findEvents(projectPath:       projects.Path,
                          maybeStatusFilter: Option[EventStatus],
                          pagingRequest:     PagingRequest
  ): Interpretation[PagingResponse[EventInfo]] = {
    implicit val finder: PagedResultsFinder[Interpretation, EventInfo] =
      createFinder(projectPath, pagingRequest, maybeStatusFilter)
    findPage(pagingRequest)
  }

  private def createFinder(projectPath:       projects.Path,
                           pagingRequest:     PagingRequest,
                           maybeStatusFilter: Option[EventStatus]
  ) =
    new PagedResultsFinder[Interpretation, EventInfo] with TypeSerializers {
      override def findResults(paging: PagingRequest): Interpretation[List[EventInfo]] =
        sessionResource.useK {
          for {
            infos               <- find()
            withProcessingTimes <- infos.map(addProcessingTimes()).sequence
          } yield withProcessingTimes
        }

      private def find() = measureExecutionTime {

        val selectStatement: Fragment[projects.Path] =
          sql"""
              SELECT evt.event_id, evt.status, evt.event_date, evt.execution_date, evt.message,  COUNT(times.status)
              FROM event evt
              JOIN project prj ON evt.project_id = prj.project_id AND prj.project_path = $projectPathEncoder
              LEFT JOIN status_processing_time times ON evt.event_id = times.event_id AND evt.project_id = times.project_id
              """

        val filterStatus: Fragment[EventStatus] =
          sql""" evt.status = $eventStatusEncoder """

        val join: Fragment[Int ~ PerPage] =
          sql""" 
              GROUP BY evt.event_id, evt.status, evt.event_date, evt.execution_date, evt.message
              ORDER BY evt.event_date DESC, evt.event_id
              OFFSET $int4
              LIMIT $perPageEncoder
          """

        val condition: List[AppliedFragment] = List(maybeStatusFilter.map(filterStatus)).flatten
        val filter =
          if (condition.isEmpty) AppliedFragment.empty
          else condition.foldSmash(void" WHERE ", AppliedFragment.empty, AppliedFragment.empty)

        val query: AppliedFragment = selectStatement(projectPath) |+| filter |+| join(
          ((pagingRequest.page.value - 1) * pagingRequest.perPage.value) ~ pagingRequest.perPage
        )

        SqlStatement[Interpretation](name = "find event infos")
          .select[query.A, (EventInfo, Long)](
            query.fragment
              .query(
                eventIdDecoder ~ eventStatusDecoder ~ eventDateDecoder ~ executionDateDecoder ~ eventMessageDecoder.opt ~ int8
              )
              .map {
                case (eventId: EventId) ~
                    (status:   EventStatus) ~
                    (eventDate: EventDate) ~
                    (executionDate: ExecutionDate) ~
                    (maybeMessage: Option[EventMessage]) ~
                    (processingTimesCount: Long) =>
                  EventInfo(eventId,
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
          .arguments(eventInfo.eventId, projectPath)
          .build(_.toList)
      }

      override def findTotal(): Interpretation[Total] = sessionResource.useK {
        measureExecutionTime {
          SqlStatement[Interpretation](name = "find event infos total")
            .select[projects.Path, Total](
              sql"""SELECT COUNT(DISTINCT evt.event_id)
              FROM event evt
              JOIN project prj ON evt.project_id = prj.project_id AND prj.project_path = $projectPathEncoder
          """
                .query(int8)
                .map((total: Long) => Total(total.toInt))
            )
            .arguments(projectPath)
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
