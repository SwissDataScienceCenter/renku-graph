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

package io.renku.eventlog.latestevents

import cats.data.Kleisli
import cats.effect.{Async, Bracket, IO}
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.events.{EventBody, EventId}
import ch.datascience.metrics.LabeledHistogram
import io.renku.eventlog.latestevents.LatestEventsFinder.IdProjectBody
import io.renku.eventlog.{EventLogDB, TypeSerializers}
import skunk._
import skunk.implicits._

trait LatestEventsFinder[Interpretation[_]] {
  def findAllLatestEvents(): Interpretation[List[IdProjectBody]]
}

class LatestEventsFinderImpl[Interpretation[_]: Async](
    transactor:       SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name]
)(implicit ME:        Bracket[Interpretation, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with LatestEventsFinder[Interpretation]
    with TypeSerializers {

  import LatestEventsFinder._
  import eu.timepit.refined.auto._

  override def findAllLatestEvents(): Interpretation[List[IdProjectBody]] =
    transactor.use(implicit session => measureExecutionTime(findEvents))

  private def findEvents = SqlQuery(
    Kleisli[Interpretation, Session[Interpretation], List[IdProjectBody]] { session =>
      val query: Query[skunk.Void, IdProjectBody] =
        sql"""SELECT evt.event_id, evt.project_id, prj.project_path, evt.event_body
          FROM event evt
          JOIN project prj ON evt.project_id = prj.project_id AND evt.event_date = prj.latest_event_date
          """
          .query(eventIdGet ~ projectGet ~ eventBodyGet)
          .map { case id ~ project ~ body => (id, project, body) }
      session.execute(query)
    },
    name = "latest projects events"
  )
}

object LatestEventsFinder {
  type IdProjectBody = (EventId, Project, EventBody)
}

object IOLatestEventsFinder {
  def apply(
      transactor:       SessionResource[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
  ): IO[LatestEventsFinder[IO]] = IO {
    new LatestEventsFinderImpl(transactor, queriesExecTimes)
  }
}
