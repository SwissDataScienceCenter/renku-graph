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

import cats.effect.{BracketThrow, Concurrent, ConcurrentEffect, IO}
import ch.datascience.db.{DbClient, SessionResource, SqlStatement}
import ch.datascience.graph.model.events.{EventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import io.renku.eventlog.events.EventsEndpoint.EventInfo
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
  import skunk.implicits._

  override def findEvents(projectPath: projects.Path): Interpretation[List[EventInfo]] =
    sessionResource.useK(measureExecutionTime(find(projectPath)))

  private def find(projectPath: projects.Path) =
    SqlStatement[Interpretation](name = "find event infos")
      .select[projects.Path, EventInfo](
        sql"""SELECT evt.event_id, evt.status, evt.message
              FROM event evt
              JOIN project prj ON evt.project_id = prj.project_id AND prj.project_path = $projectPathEncoder
              ORDER BY evt.event_date
          """
          .query(eventIdDecoder ~ eventStatusDecoder ~ eventMessageDecoder.opt)
          .map { case (eventId: EventId) ~ (status: EventStatus) ~ (maybeMessage: Option[EventMessage]) =>
            EventInfo(eventId, status, maybeMessage)
          }
      )
      .arguments(projectPath)
      .build(_.toList)
}

private object EventsFinder {
  def apply(sessionResource:   SessionResource[IO, EventLogDB],
            queriesExecTimes:  LabeledHistogram[IO, SqlStatement.Name]
  )(implicit concurrentEffect: ConcurrentEffect[IO]): IO[EventsFinder[IO]] = IO(
    new EventsFinderImpl(sessionResource, queriesExecTimes)
  )
}
