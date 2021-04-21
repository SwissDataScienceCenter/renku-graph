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

import cats.data.Kleisli
import cats.effect.{Async, Bracket}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, New, TransformingTriples, TriplesGenerated}
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.{EventLogDB, ExecutionDate, TypeSerializers}
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

private trait ZombieStatusCleaner[Interpretation[_]] {
  def cleanZombieStatus(event: ZombieEvent): Interpretation[UpdateResult]
}

private class ZombieStatusCleanerImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with ZombieStatusCleaner[Interpretation]
    with TypeSerializers {

  override def cleanZombieStatus(event: ZombieEvent): Interpretation[UpdateResult] = sessionResource.useK {
    cleanEventualDeliveries(event.eventId) >> updateEventStatus(event)
  }

  private lazy val updateEventStatus: ZombieEvent => Kleisli[Interpretation, Session[Interpretation], UpdateResult] = {
    case GeneratingTriplesZombieEvent(eventId, _)   => updateStatusQuery(eventId, GeneratingTriples, New)
    case TransformingTriplesZombieEvent(eventId, _) => updateStatusQuery(eventId, TransformingTriples, TriplesGenerated)
  }

  private def cleanEventualDeliveries(eventId: CompoundEventId) =
    measureExecutionTime {
      SqlQuery(
        Kleisli { session =>
          val query: Command[EventId ~ projects.Id] =
            sql"""DELETE FROM event_delivery
                  WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
            """.command
          session.prepare(query).use(_ execute (eventId.id ~ eventId.projectId))
        },
        name = "zombie_chasing - clean deliveries"
      )
    }

  private def updateStatusQuery(
      eventId:   CompoundEventId,
      oldStatus: EventStatus,
      newStatus: EventStatus
  ) = measureExecutionTime {
    SqlQuery(
      Kleisli { session =>
        val query: Command[EventStatus ~ ExecutionDate ~ EventId ~ projects.Id ~ EventStatus] =
          sql"""UPDATE event
                SET status = $eventStatusEncoder, execution_date = $executionDateEncoder, message = NULL
                WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder AND status = $eventStatusEncoder
          """.command
        session.prepare(query).use {
          _.execute(newStatus ~ ExecutionDate(now()) ~ eventId.id ~ eventId.projectId ~ oldStatus).flatMap {
            case Completion.Update(1) => (Updated: UpdateResult).pure[Interpretation]
            case Completion.Update(0) => (NotUpdated: UpdateResult).pure[Interpretation]
            case _ =>
              new Exception(s"${categoryName.value} - zombie_chasing - update status- More than one row updated")
                .raiseError[Interpretation, UpdateResult]
          }
        }
      },
      name = "zombie_chasing - update status"
    )
  }
}

private object ZombieStatusCleaner {

  import cats.effect.IO

  def apply(sessionResource:  SessionResource[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
  ): IO[ZombieStatusCleaner[IO]] = IO(new ZombieStatusCleanerImpl(sessionResource, queriesExecTimes))
}
