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
import cats.effect.{Async, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.metrics.LabeledHistogram
import io.renku.eventlog.{EventLogDB, TypeSerializers}
import skunk.implicits._
import skunk.codec.all._

private trait EventDetailsFinder[Interpretation[_]] {
  def findDetails(eventId: CompoundEventId): Interpretation[Option[CompoundEventId]]
}

private class EventDetailsFinderImpl[Interpretation[_]: Async](
    transactor:       SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name]
) extends DbClient[Interpretation](Some(queriesExecTimes))
    with EventDetailsFinder[Interpretation]
    with TypeSerializers {

  import eu.timepit.refined.auto._

  override def findDetails(eventId: CompoundEventId): Interpretation[Option[CompoundEventId]] =
    transactor.use { session =>
      session.transaction.use { xa =>
        for {
          sp <- xa.savepoint
          result <- measureExecutionTime(find(eventId), session).recoverWith { case e =>
                      xa.rollback(sp).flatMap(_ => e.raiseError[Interpretation, Option[CompoundEventId]])
                    }
        } yield result
      }
    }

  private def find(eventId: CompoundEventId) = SqlQuery[Interpretation, Option[CompoundEventId]](
    Kleisli(_.option(sql"""SELECT evt.event_id, evt.project_id
                           FROM event evt WHERE evt.event_id = #${eventId.id} and evt.project_id = #${eventId.projectId.toString}
                           """.query(varchar ~ int4).gmap[CompoundEventId])),
    name = "find event details"
  )
}

private object EventDetailsFinder {
  def apply(
      transactor:       SessionResource[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
  ): IO[EventDetailsFinder[IO]] = IO {
    new EventDetailsFinderImpl(transactor, queriesExecTimes)
  }
}
