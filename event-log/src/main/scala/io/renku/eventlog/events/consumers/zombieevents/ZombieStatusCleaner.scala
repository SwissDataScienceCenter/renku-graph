/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.zombieevents

import cats.MonadThrow
import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, Deleting, GeneratingTriples, New, TransformingTriples, TriplesGenerated}
import io.renku.graph.model.events.{CompoundEventId, EventId, EventStatus, ExecutionDate}
import io.renku.graph.model.projects
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

private trait ZombieStatusCleaner[F[_]] {
  def cleanZombieStatus(event: ZombieEvent): F[UpdateResult]
}

private class ZombieStatusCleanerImpl[F[_]: Async: SessionResource: QueriesExecutionTimes](
    now: () => Instant = () => Instant.now
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with ZombieStatusCleaner[F]
    with TypeSerializers {

  override def cleanZombieStatus(event: ZombieEvent): F[UpdateResult] = SessionResource[F].useK {
    cleanEventualDeliveries(event.eventId) >> updateEventStatus(event)
  }

  private lazy val updateEventStatus: ZombieEvent => Kleisli[F, Session[F], UpdateResult] = {
    case ZombieEvent(eventId, _, GeneratingTriples)   => runUpdate(eventId, GeneratingTriples, New)
    case ZombieEvent(eventId, _, TransformingTriples) => runUpdate(eventId, TransformingTriples, TriplesGenerated)
    case ZombieEvent(eventId, _, Deleting)            => runUpdate(eventId, Deleting, AwaitingDeletion)
  }

  private def cleanEventualDeliveries(eventId: CompoundEventId) = measureExecutionTime {
    SqlStatement(name = "zombie_chasing - clean deliveries")
      .command[EventId *: projects.GitLabId *: EmptyTuple](
        sql"""DELETE FROM event_delivery
              WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
            """.command
      )
      .arguments(eventId.id *: eventId.projectId *: EmptyTuple)
      .build
  }

  private def runUpdate(eventId: CompoundEventId, oldStatus: EventStatus, newStatus: EventStatus) =
    measureExecutionTime {
      SqlStatement(name = "zombie_chasing - update status")
        .command[EventStatus *: ExecutionDate *: EventId *: projects.GitLabId *: EventStatus *: EmptyTuple](sql"""
          UPDATE event
          SET status = $eventStatusEncoder, execution_date = $executionDateEncoder, message = NULL
          WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder AND status = $eventStatusEncoder
          """.command)
        .arguments(newStatus *: ExecutionDate(now()) *: eventId.id *: eventId.projectId *: oldStatus *: EmptyTuple)
        .build
        .flatMapResult {
          case Completion.Update(1) => (Updated: UpdateResult).pure[F]
          case Completion.Update(0) => (NotUpdated: UpdateResult).pure[F]
          case completion => new Exception(s"$categoryName: update status $completion").raiseError[F, UpdateResult]
        }
    }
}

private object ZombieStatusCleaner {
  def apply[F[_]: Async: SessionResource: QueriesExecutionTimes]: F[ZombieStatusCleaner[F]] =
    MonadThrow[F].catchNonFatal(new ZombieStatusCleanerImpl[F]())
}
