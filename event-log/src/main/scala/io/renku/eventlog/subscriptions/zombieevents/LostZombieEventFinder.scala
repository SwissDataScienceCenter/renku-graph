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
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
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
import scala.language.postfixOps

private class LostZombieEventFinder[Interpretation[_]: Async: Bracket[*[_], Throwable]: ContextShift](
    transactor:       SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name]
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[Interpretation, ZombieEvent]
    with ZombieEventSubProcess
    with TypeSerializers {

  override def popEvent(): Interpretation[Option[ZombieEvent]] = transactor.use { implicit session =>
    session.transaction.use { transaction =>
      for {
        sp <- transaction.savepoint
        result <- (findEvent >>= markEventTaken) recoverWith { error =>
                    transaction.rollback(sp).flatMap(_ => error.raiseError[Interpretation, Option[ZombieEvent]])
                  }
      } yield result
    }
  }
  private val maxDurationForEvent = EventProcessingTime(Duration.ofMinutes(5))

  private def findEvent(implicit session: Session[Interpretation]) = measureExecutionTime {
    SqlQuery(
      Kleisli { session =>
        val query: Query[EventStatus ~ EventStatus ~ String ~ ExecutionDate ~ EventProcessingTime, ZombieEvent] =
          sql"""SELECT evt.event_id, evt.project_id, proj.project_path, evt.status
                FROM event evt
                JOIN project proj ON evt.project_id = proj.project_id
                WHERE (evt.status = $eventStatusPut
                  OR evt.status = $eventStatusPut)
                  AND evt.message = $text
                  AND  (($executionDatePut - evt.execution_date) > $eventProcessingTimePut)
                LIMIT 1
          """
            .query(eventIdGet ~ projectIdGet ~ projectPathGet ~ eventStatusGet)
            .map { case eventId ~ projectId ~ path ~ status =>
              ZombieEvent(processName, CompoundEventId(eventId, projectId), path, status)
            }

        session
          .prepare(query)
          .use(
            _.option(
              GeneratingTriples ~ TransformingTriples ~ zombieMessage ~ ExecutionDate(now()) ~ maxDurationForEvent
            )
          )
      },
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lze - find event")
    )
  }

  private def markEventTaken(implicit
      session: Session[Interpretation]
  ): Option[ZombieEvent] => Interpretation[Option[ZombieEvent]] = {
    case None        => Option.empty[ZombieEvent].pure[Interpretation]
    case Some(event) => updateExecutionDate(event.eventId) map toNoneIfEventAlreadyTaken(event)
  }

  private def toNoneIfEventAlreadyTaken(event: ZombieEvent): Completion => Option[ZombieEvent] = {
    case Completion.Update(0) => None
    case Completion.Update(1) => Some(event)
    case completion =>
      throw new Exception(
        s"${categoryName.value.toLowerCase} - lze - Query failed with status $completion"
      ) // TODO verify
  }

  private def updateExecutionDate(eventId: CompoundEventId)(implicit session: Session[Interpretation]) =
    measureExecutionTime {
      SqlQuery(
        Kleisli { session =>
          val query: Command[ExecutionDate ~ EventId ~ projects.Id ~ String] = sql"""
              UPDATE event
              SET execution_date = $executionDatePut
              WHERE event_id = $eventIdPut AND project_id = $projectIdPut AND message = $text
              """.command
          session.prepare(query).use(_.execute(ExecutionDate(now()) ~ eventId.id ~ eventId.projectId ~ zombieMessage))
        },
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lze - update execution date")
      )
    }

  override val processName: ZombieEventProcess = ZombieEventProcess("lze")
}

private object LostZombieEventFinder {
  def apply(
      transactor:          SessionResource[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift: ContextShift[IO]): IO[EventFinder[IO, ZombieEvent]] = IO {
    new LostZombieEventFinder(transactor, queriesExecTimes)
  }
}
