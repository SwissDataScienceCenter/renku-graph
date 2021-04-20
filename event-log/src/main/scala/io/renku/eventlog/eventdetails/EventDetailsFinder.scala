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

import cats.data.Kleisli
import cats.syntax.all._
import cats.effect.{Async, IO}
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.events.{CompoundEventId, EventId}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import io.renku.eventlog.{EventLogDB, TypeSerializers}
import skunk.{Decoder, _}
import skunk.codec.all._
import skunk.implicits._

private trait EventDetailsFinder[Interpretation[_]] {
  def findDetails(eventId: CompoundEventId): Interpretation[Option[CompoundEventId]]
}

private class EventDetailsFinderImpl[Interpretation[_]: Async](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name]
) extends DbClient[Interpretation](Some(queriesExecTimes))
    with EventDetailsFinder[Interpretation]
    with TypeSerializers {

  import eu.timepit.refined.auto._

  override def findDetails(eventId: CompoundEventId): Interpretation[Option[CompoundEventId]] =
    sessionResource.useK(measureExecutionTimeK(find(eventId)))

  private def find(eventId: CompoundEventId) =
    SqlQuery[Interpretation, Option[CompoundEventId]](
      Kleisli { session =>
        val query: Query[EventId ~ projects.Id, CompoundEventId] =
          sql"""SELECT evt.event_id, evt.project_id
                           FROM event evt WHERE evt.event_id = $eventIdPut and evt.project_id = $projectIdPut
                           """.query(compoundEventIdGet)
        session.prepare(query).use(_.option(eventId.id ~ eventId.projectId))
      },
      name = "find event details"
    )
}

private object EventDetailsFinder {
  def apply(
      sessionResource:  SessionResource[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
  ): IO[EventDetailsFinder[IO]] = IO {
    new EventDetailsFinderImpl(sessionResource, queriesExecTimes)
  }
}
