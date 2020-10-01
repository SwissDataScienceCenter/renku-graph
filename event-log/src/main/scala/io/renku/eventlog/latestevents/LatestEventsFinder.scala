/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import cats.effect.{Bracket, IO}
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.{EventBody, EventId}
import ch.datascience.metrics.LabeledHistogram
import doobie.implicits._
import io.renku.eventlog.latestevents.LatestEventsFinder.IdProjectBody
import io.renku.eventlog.{EventLogDB, EventProject, TypesSerializers}

trait LatestEventsFinder[Interpretation[_]] {
  def findAllLatestEvents(): Interpretation[List[IdProjectBody]]
}

class LatestEventsFinderImpl(
    transactor:       DbTransactor[IO, EventLogDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
)(implicit ME:        Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with LatestEventsFinder[IO]
    with TypesSerializers {

  import LatestEventsFinder._
  import eu.timepit.refined.auto._

  override def findAllLatestEvents(): IO[List[IdProjectBody]] =
    measureExecutionTime(findEvents) transact transactor.get

  private def findEvents = SqlQuery(
    sql"""|select log.event_id, log.project_id, log.project_path, log.event_body
          |from event_log log, (select project_id, max(event_date) max_event_date from event_log group by project_id) aggregate
          |where log.project_id = aggregate.project_id and log.event_date = aggregate.max_event_date
          |""".stripMargin
      .query[(EventId, EventProject, EventBody)]
      .to[List],
    name = "latest projects events"
  )
}

object LatestEventsFinder {
  type IdProjectBody = (EventId, EventProject, EventBody)
}

object IOLatestEventsFinder {
  def apply(
      transactor:       DbTransactor[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
  ): IO[LatestEventsFinder[IO]] = IO {
    new LatestEventsFinderImpl(transactor, queriesExecTimes)
  }
}
