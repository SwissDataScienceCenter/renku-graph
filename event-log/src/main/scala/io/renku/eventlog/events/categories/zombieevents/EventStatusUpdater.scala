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

package io.renku.eventlog.events.categories.zombieevents

import cats.syntax.all._
import cats.effect.{Bracket, IO}
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, New, TransformingTriples, TriplesGenerated}
import ch.datascience.metrics.LabeledHistogram
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.{EventLogDB, TypeSerializers}

import java.time.Instant

private trait EventStatusUpdater[Interpretation[_]] {
  def changeStatus(event: ZombieEvent): Interpretation[UpdateResult]
}

private class EventStatusUpdaterImpl(
    transactor:       DbTransactor[IO, EventLogDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
    now:              () => Instant = () => Instant.now
)(implicit ME:        Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with EventStatusUpdater[IO]
    with TypeSerializers {

  override def changeStatus(event: ZombieEvent): IO[UpdateResult] =
    (measureExecutionTime(query(event)) transact transactor.get) >>= {
      case 1 => Updated.pure[IO]
      case 0 => NotUpdated.pure[IO]
      case _ => new Exception("More than one row updated").raiseError[IO, UpdateResult]
    }

  private lazy val query: ZombieEvent => SqlQuery[Int] = {
    case GeneratingTriplesZombieEvent(eventId, _)   => createQuery(eventId, GeneratingTriples, New)
    case TransformingTriplesZombieEvent(eventId, _) => createQuery(eventId, TransformingTriples, TriplesGenerated)
  }

  private def createQuery(eventId: CompoundEventId, oldStatus: EventStatus, newStatus: EventStatus) = SqlQuery(
    query = sql"""
                 | UPDATE event SET status = $newStatus, execution_date = ${now()}
                 | WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId} AND status = $oldStatus
                 |""".stripMargin.update.run,
    name = "zombie_chasing - update status"
  )
}

private object EventStatusUpdater {
  import cats.effect.IO

  def apply(transactor:       DbTransactor[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
  ): IO[EventStatusUpdater[IO]] = IO {
    new EventStatusUpdaterImpl(transactor, queriesExecTimes)
  }

}
