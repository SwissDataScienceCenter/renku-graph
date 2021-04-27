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

package io.renku.eventlog.subscriptions.zombieevents

import cats.data.Kleisli
import cats.effect.{Async, Bracket, ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlStatement}
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, TransformingTriples}
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.api.Refined
import io.renku.eventlog.subscriptions.EventFinder
import io.renku.eventlog.{EventLogDB, ExecutionDate, TypeSerializers}
import skunk._
import skunk.codec.all._
import skunk.data.Completion
import skunk.implicits._

import java.time.Duration
import java.time.Instant.now

private class LostZombieEventFinder[Interpretation[_]: Async: Bracket[*[_], Throwable]: ContextShift](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name]
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[Interpretation, ZombieEvent]
    with ZombieEventSubProcess
    with TypeSerializers {

  override def popEvent(): Interpretation[Option[ZombieEvent]] = sessionResource.useK {
    findEvent >>= markEventTaken
  }
  private val maxDurationForEvent = EventProcessingTime(Duration.ofMinutes(5))

  private lazy val findEvent = measureExecutionTime {
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

  private lazy val markEventTaken
      : Option[ZombieEvent] => Kleisli[Interpretation, Session[Interpretation], Option[ZombieEvent]] = {
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
          case Completion.Update(1) => true.pure[Interpretation]
          case Completion.Update(0) => false.pure[Interpretation]
          case completion =>
            new Exception(
              s"${categoryName.value.toLowerCase} - lze - update execution date failed with status $completion"
            ).raiseError[Interpretation, Boolean]
        }
    }

  override val processName: ZombieEventProcess = ZombieEventProcess("lze")
}

private object LostZombieEventFinder {
  def apply(
      sessionResource:     SessionResource[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlStatement.Name]
  )(implicit contextShift: ContextShift[IO]): IO[EventFinder[IO, ZombieEvent]] = IO {
    new LostZombieEventFinder(sessionResource, queriesExecTimes)
  }
}
