/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions.zombieevents

import cats.MonadThrow
import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.subscriptions.EventFinder
import io.renku.eventlog.{ExecutionDate, TypeSerializers}
import io.renku.graph.model.events.EventStatus.{GeneratingTriples, TransformingTriples}
import io.renku.graph.model.events.{CompoundEventId, EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import skunk._
import skunk.codec.all._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant.now

private class LostSubscriberEventFinder[F[_]: MonadCancelThrow: SessionResource](
    queriesExecTimes: LabeledHistogram[F]
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[F, ZombieEvent]
    with ZombieEventSubProcess
    with TypeSerializers {

  override val processName: ZombieEventProcess = ZombieEventProcess("lse")

  override def popEvent(): F[Option[ZombieEvent]] = SessionResource[F].useK {
    findEvents >>= markEventTaken()
  }

  private lazy val findEvents = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - lse - find events")
      .select[EventStatus ~ EventStatus ~ String, ZombieEvent](
        sql"""
        SELECT DISTINCT evt.event_id, evt.project_id, proj.project_path, evt.status
        FROM event_delivery delivery
        JOIN event evt ON evt.event_id = delivery.event_id
          AND evt.project_id = delivery.project_id
          AND (evt.status = $eventStatusEncoder OR evt.status = $eventStatusEncoder)
          AND (evt.message IS NULL OR evt.message <> $text)
        JOIN project proj ON evt.project_id = proj.project_id
        WHERE NOT EXISTS (
          SELECT sub.delivery_id
          FROM subscriber sub
          WHERE sub.delivery_id = delivery.delivery_id
        )
        LIMIT 1
        """
          .query(eventIdDecoder ~ projectIdDecoder ~ projectPathDecoder ~ eventStatusDecoder)
          .map { case eventId ~ projectId ~ projectPath ~ status =>
            ZombieEvent(processName, CompoundEventId(eventId, projectId), projectPath, status)
          }
      )
      .arguments(GeneratingTriples ~ TransformingTriples ~ zombieMessage)
      .build(_.option)
  }

  private def markEventTaken(): Option[ZombieEvent] => Kleisli[F, Session[F], Option[ZombieEvent]] = {
    case None        => Kleisli.pure(Option.empty[ZombieEvent])
    case Some(event) => updateMessage(event.eventId) map toNoneIfEventAlreadyTaken(event)
  }

  private def updateMessage(eventId: CompoundEventId) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - lse - update message")
      .command[String ~ ExecutionDate ~ EventId ~ projects.Id](sql"""
        UPDATE event
        SET message = $text, execution_date = $executionDateEncoder
        WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
        """.command)
      .arguments(
        zombieMessage ~ ExecutionDate(now) ~ eventId.id ~ eventId.projectId
      )
      .build
      .flatMapResult {
        case Completion.Update(0) => false.pure[F]
        case Completion.Update(1) => true.pure[F]
        case completion =>
          new Exception(
            s"${categoryName.value.toLowerCase} - lse - update message failed with status $completion"
          ).raiseError[F, Boolean]
      }
  }

  private def toNoneIfEventAlreadyTaken(event: ZombieEvent): Boolean => Option[ZombieEvent] = {
    case true  => Some(event)
    case false => None
  }
}

private object LostSubscriberEventFinder {
  def apply[F[_]: MonadCancelThrow: SessionResource](
      queriesExecTimes: LabeledHistogram[F]
  ): F[EventFinder[F, ZombieEvent]] = MonadThrow[F].catchNonFatal {
    new LostSubscriberEventFinder[F](queriesExecTimes)
  }
}
