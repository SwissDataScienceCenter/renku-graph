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

import cats.effect.{Bracket, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, New, TransformingTriples, TriplesGenerated}
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.metrics.LabeledHistogram
import doobie.ConnectionIO
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.{EventLogDB, TypeSerializers}

import java.time.Instant

private trait ZombieStatusCleaner[Interpretation[_]] {
  def cleanZombieStatus(event: ZombieEvent): Interpretation[UpdateResult]
}

private class ZombieStatusCleanerImpl(
    transactor:       SessionResource[IO, EventLogDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
    now:              () => Instant = () => Instant.now
)(implicit ME:        Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with ZombieStatusCleaner[IO]
    with TypeSerializers {

  override def cleanZombieStatus(event: ZombieEvent): IO[UpdateResult] = {
    for {
      _      <- cleanEventualDeliveries(event.eventId)
      result <- updateEventStatus(event)
    } yield result
  } transact transactor.resource flatMap toResult

  private lazy val updateEventStatus: ZombieEvent => ConnectionIO[Int] = {
    case GeneratingTriplesZombieEvent(eventId, _)   => updateStatusQuery(eventId, GeneratingTriples, New)
    case TransformingTriplesZombieEvent(eventId, _) => updateStatusQuery(eventId, TransformingTriples, TriplesGenerated)
  }

  private def cleanEventualDeliveries(eventId: CompoundEventId) = measureExecutionTime {
    SqlQuery(
      sql"""|DELETE FROM event_delivery
            |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId}
            |""".stripMargin.update.run,
      name = "zombie_chasing - clean deliveries"
    )
  }

  private def updateStatusQuery(
      eventId:   CompoundEventId,
      oldStatus: EventStatus,
      newStatus: EventStatus
  ) = measureExecutionTime {
    SqlQuery(
      sql"""|UPDATE event
            |SET status = $newStatus, execution_date = ${now()}, message = NULL
            |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId} AND status = $oldStatus
            |""".stripMargin.update.run,
      name = "zombie_chasing - update status"
    )
  }

  private lazy val toResult: Int => IO[UpdateResult] = {
    case 1 => Updated.pure[IO]
    case 0 => NotUpdated.pure[IO]
    case _ => new Exception("More than one row updated").raiseError[IO, UpdateResult]
  }
}

private object ZombieStatusCleaner {
  import cats.effect.IO

  def apply(transactor:       SessionResource[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
  ): IO[ZombieStatusCleaner[IO]] = IO {
    new ZombieStatusCleanerImpl(transactor, queriesExecTimes)
  }
}
