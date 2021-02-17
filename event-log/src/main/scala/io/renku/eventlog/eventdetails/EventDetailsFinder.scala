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

package io.renku.eventlog.eventdetails

import cats.effect.{Bracket, IO}
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.metrics.LabeledHistogram
import doobie.implicits._
import io.renku.eventlog.{EventLogDB, TypeSerializers}

private trait EventDetailsFinder[Interpretation[_]] {
  def findDetails(eventId: CompoundEventId): Interpretation[Option[CompoundEventId]]
}

private class EventDetailsFinderImpl(
    transactor:       DbTransactor[IO, EventLogDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
)(implicit ME:        Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with EventDetailsFinder[IO]
    with TypeSerializers {

  import eu.timepit.refined.auto._

  override def findDetails(eventId: CompoundEventId): IO[Option[CompoundEventId]] =
    measureExecutionTime(find(eventId)) transact transactor.get

  private def find(eventId: CompoundEventId) = SqlQuery(
    sql"""|SELECT evt.event_id, evt.project_id
          |FROM event evt WHERE evt.event_id = ${eventId.id} and evt.project_id = ${eventId.projectId} 
          |""".stripMargin
      .query[CompoundEventId]
      .option,
    name = "find event details"
  )
}

private object EventDetailsFinder {
  def apply(
      transactor:       DbTransactor[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
  ): IO[EventDetailsFinder[IO]] = IO {
    new EventDetailsFinderImpl(transactor, queriesExecTimes)
  }
}
