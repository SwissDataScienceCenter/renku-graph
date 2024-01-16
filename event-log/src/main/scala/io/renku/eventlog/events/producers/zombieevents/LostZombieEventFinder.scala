/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.producers.zombieevents

import cats.MonadThrow
import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.events.producers
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.model.events.EventStatus.ProcessingStatus
import io.renku.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, ExecutionDate}
import io.renku.graph.model.projects
import skunk._
import skunk.codec.all._
import skunk.data.Completion
import skunk.implicits._

import java.time.Duration
import java.time.Instant.now

private class LostZombieEventFinder[F[_]: Async: SessionResource: QueriesExecutionTimes]
    extends DbClient(Some(QueriesExecutionTimes[F]))
    with producers.EventFinder[F, ZombieEvent]
    with ZombieEventSubProcess
    with TypeSerializers {

  override val processName: ZombieEventProcess = ZombieEventProcess("lze")

  override def popEvent(): F[Option[ZombieEvent]] = SessionResource[F].useK {
    findEvent >>= markEventTaken
  }
  private val maxDurationForEvent = EventProcessingTime(Duration.ofMinutes(5))

  private def findEvent = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - lze - find event")
      .select[ExecutionDate *: EventProcessingTime *: EmptyTuple, ZombieEvent](
        sql"""SELECT evt.event_id, evt.project_id, proj.project_slug, evt.status
              FROM event evt
              JOIN project proj ON evt.project_id = proj.project_id
              WHERE 
                evt.status IN (#${ProcessingStatus.all.map(s => s"'${s.value}'").mkString(", ")})
                AND evt.message = '#$zombieMessage'
                AND  (($executionDateEncoder - evt.execution_date) > $eventProcessingTimeEncoder)
              LIMIT 1
          """
          .query(eventIdDecoder ~ projectIdDecoder ~ projectSlugDecoder ~ processingStatusDecoder)
          .map { case eventId ~ projectId ~ slug ~ status =>
            ZombieEvent(processName, CompoundEventId(eventId, projectId), slug, status)
          }
      )
      .arguments(ExecutionDate(now()) *: maxDurationForEvent *: EmptyTuple)
      .build(_.option)
  }

  private lazy val markEventTaken: Option[ZombieEvent] => Kleisli[F, Session[F], Option[ZombieEvent]] = {
    case None        => Kleisli.pure(Option.empty[ZombieEvent])
    case Some(event) => updateExecutionDate(event.eventId) map toNoneIfEventAlreadyTaken(event)
  }

  private def toNoneIfEventAlreadyTaken(event: ZombieEvent): Boolean => Option[ZombieEvent] = {
    case true  => Some(event)
    case false => None
  }

  private def updateExecutionDate(eventId: CompoundEventId) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - lze - update execution date")
      .command[ExecutionDate *: EventId *: projects.GitLabId *: String *: EmptyTuple](sql"""
        UPDATE event
        SET execution_date = $executionDateEncoder
        WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder AND message = $text
        """.command)
      .arguments(ExecutionDate(now()) *: eventId.id *: eventId.projectId *: zombieMessage *: EmptyTuple)
      .build
      .flatMapResult {
        case Completion.Update(1) => true.pure[F]
        case Completion.Update(0) => false.pure[F]
        case completion =>
          new Exception(
            s"${categoryName.value.toLowerCase} - lze - update execution date failed with status $completion"
          ).raiseError[F, Boolean]
      }
  }
}

private object LostZombieEventFinder {
  def apply[F[_]: Async: SessionResource: QueriesExecutionTimes]: F[producers.EventFinder[F, ZombieEvent]] =
    MonadThrow[F].catchNonFatal(new LostZombieEventFinder[F])
}
