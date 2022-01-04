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
import eu.timepit.refined.api.Refined
import io.renku.db.{DbClient, SessionResource, SqlStatement}
import io.renku.eventlog.subscriptions.EventFinder
import io.renku.eventlog.{EventLogDB, ExecutionDate, TypeSerializers}
import io.renku.graph.model.events.EventStatus.{GeneratingTriples, TransformingTriples}
import io.renku.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus}
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import skunk._
import skunk.codec.all._
import skunk.data.Completion
import skunk.implicits._

import java.time.Duration
import java.time.Instant.now

private class LostZombieEventFinder[F[_]: MonadCancelThrow](
    sessionResource:  SessionResource[F, EventLogDB],
    queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[F, ZombieEvent]
    with ZombieEventSubProcess
    with TypeSerializers {

  override def popEvent(): F[Option[ZombieEvent]] = sessionResource.useK {
    findEvent >>= markEventTaken
  }
  private val maxDurationForEvent = EventProcessingTime(Duration.ofMinutes(5))

  private def findEvent = measureExecutionTime {
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lze - find event"))
      .select[EventStatus ~ EventStatus ~ String ~ ExecutionDate ~ EventProcessingTime, ZombieEvent](
        sql"""SELECT evt.event_id, evt.project_id, proj.project_path, evt.status
                FROM event evt
                JOIN project proj ON evt.project_id = proj.project_id
                WHERE (evt.status = $eventStatusEncoder
                  OR evt.status = $eventStatusEncoder)
                  AND evt.message = $text
                  AND  (($executionDateEncoder - evt.execution_date) > $eventProcessingTimeEncoder)
                LIMIT 1
          """
          .query(eventIdDecoder ~ projectIdDecoder ~ projectPathDecoder ~ eventStatusDecoder)
          .map { case eventId ~ projectId ~ path ~ status =>
            ZombieEvent(processName, CompoundEventId(eventId, projectId), path, status)
          }
      )
      .arguments(GeneratingTriples ~ TransformingTriples ~ zombieMessage ~ ExecutionDate(now()) ~ maxDurationForEvent)
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

  private def updateExecutionDate(eventId: CompoundEventId) =
    measureExecutionTime {
      SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lze - update execution date"))
        .command[ExecutionDate ~ EventId ~ projects.Id ~ String](
          sql"""UPDATE event
                  SET execution_date = $executionDateEncoder
                  WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder AND message = $text
            """.command
        )
        .arguments(ExecutionDate(now()) ~ eventId.id ~ eventId.projectId ~ zombieMessage)
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

  override val processName: ZombieEventProcess = ZombieEventProcess("lze")
}

private object LostZombieEventFinder {
  def apply[F[_]: MonadCancelThrow](
      sessionResource:  SessionResource[F, EventLogDB],
      queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ): F[EventFinder[F, ZombieEvent]] = MonadThrow[F].catchNonFatal {
    new LostZombieEventFinder(sessionResource, queriesExecTimes)
  }
}
