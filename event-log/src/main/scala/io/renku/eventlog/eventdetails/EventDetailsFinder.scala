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

import cats.effect.{BracketThrow, IO}
import ch.datascience.db.{DbClient, SessionResource, SqlStatement}
import ch.datascience.graph.model.events.{CompoundEventId, EventBody, EventDetails, EventId}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import io.renku.eventlog.{EventLogDB, TypeSerializers}
import skunk._
import skunk.implicits._

private trait EventDetailsFinder[Interpretation[_]] {
  def findDetails(eventId: CompoundEventId): Interpretation[Option[EventDetails]]
}

private class EventDetailsFinderImpl[Interpretation[_]: BracketThrow](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name]
) extends DbClient[Interpretation](Some(queriesExecTimes))
    with EventDetailsFinder[Interpretation]
    with TypeSerializers {

  import eu.timepit.refined.auto._

  override def findDetails(eventId: CompoundEventId): Interpretation[Option[EventDetails]] =
    sessionResource.useK(measureExecutionTime(find(eventId)))

  private def find(eventId: CompoundEventId) =
    SqlStatement[Interpretation](name = "find event details")
      .select[EventId ~ projects.Id, EventDetails](
        sql"""SELECT evt.event_id, evt.project_id, evt.event_body
                FROM event evt WHERE evt.event_id = $eventIdEncoder and evt.project_id = $projectIdEncoder
          """.query(compoundEventIdDecoder ~ eventBodyDecoder).map {
          case (eventId: CompoundEventId, eventBody: EventBody) => EventDetails(eventId, eventBody)
        }
      )
      .arguments(eventId.id ~ eventId.projectId)
      .build(_.option)

}

private object EventDetailsFinder {
  def apply(
      sessionResource:  SessionResource[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlStatement.Name]
  ): IO[EventDetailsFinder[IO]] = IO {
    new EventDetailsFinderImpl(sessionResource, queriesExecTimes)
  }
}
