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
import ch.datascience.metrics.LabeledHistogram
import io.renku.eventlog.events.EventsEndpoint.{EventInfo, StatusProcessingTime}
import io.renku.eventlog.{EventLogDB, EventMessage, TypeSerializers}

private trait EventsFinder[Interpretation[_]] {
  def findEvents(projectPath: projects.Path): Interpretation[List[EventInfo]]
}

private class EventsFinderImpl[Interpretation[_]: BracketThrow: Concurrent](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name]
) extends DbClient[Interpretation](Some(queriesExecTimes))
    with EventsFinder[Interpretation]
    with TypeSerializers {

  import ch.datascience.db.implicits._
  import eu.timepit.refined.auto._
  import skunk._
  import skunk.codec.numeric._
  import skunk.implicits._

  override def findEvents(projectPath: projects.Path): Interpretation[List[EventInfo]] =
    sessionResource.useK {
      for {
        infos               <- find(projectPath)
        withProcessingTimes <- infos.map(addProcessingTimes(projectPath)).sequence
      } yield withProcessingTimes
    }

  private def addProcessingTimes(
      projectPath: projects.Path
  ): ((EventInfo, Long)) => Kleisli[Interpretation, Session[Interpretation], EventInfo] = {
    case (info, 0L) => Kleisli.pure(info)
    case (info, _) =>
      findStatusProcessingTimes(projectPath, info)
        .map(processingTimes => info.copy(processingTimes = processingTimes.sortBy(_.status)))
  }

  private def find(projectPath: projects.Path) = measureExecutionTime {

    SqlStatement[Interpretation](name = "find event infos")
      .select[projects.Path, (EventInfo, Long)](
        sql"""SELECT evt.event_id, evt.status, evt.message,  COUNT(times.status)
              FROM event evt
              JOIN project prj ON evt.project_id = prj.project_id AND prj.project_path = $projectPathEncoder
              LEFT JOIN status_processing_time times ON evt.event_id = times.event_id AND evt.project_id = times.project_id
              GROUP BY evt.event_id, evt.status, evt.message, evt.event_date
              ORDER BY evt.event_date DESC, evt.event_id
          """
          .query(eventIdDecoder ~ eventStatusDecoder ~ eventMessageDecoder.opt ~ int8)
          .map {
            case (eventId: EventId) ~
                (status:   EventStatus) ~
                (maybeMessage: Option[EventMessage]) ~
                (processingTimesCount: Long) =>
              EventInfo(eventId, status, maybeMessage, processingTimes = List.empty) -> processingTimesCount
          }
      )
      .arguments(projectPath)
      .build(_.toList)
  }

  private def findStatusProcessingTimes(projectPath: projects.Path, eventInfo: EventInfo) = measureExecutionTime {
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

  private lazy val statusProcessingTimesDecoder: Decoder[StatusProcessingTime] =
    (eventStatusDecoder ~ eventProcessingTimeDecoder).map((StatusProcessingTime.apply _).tupled.apply)
}

private object EventsFinder {
  def apply(sessionResource:   SessionResource[IO, EventLogDB],
            queriesExecTimes:  LabeledHistogram[IO, SqlStatement.Name]
  )(implicit concurrentEffect: ConcurrentEffect[IO]): IO[EventsFinder[IO]] = IO(
    new EventsFinderImpl(sessionResource, queriesExecTimes)
  )
}
