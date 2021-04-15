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

import cats.effect.{ContextShift, IO}
import cats.free.Free
import cats.syntax.all._
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, TransformingTriples}
import ch.datascience.graph.model.events.{CompoundEventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import doobie.free.connection.ConnectionOp
import eu.timepit.refined.api.Refined
import io.renku.eventlog.subscriptions.EventFinder
import io.renku.eventlog.{EventLogDB, TypeSerializers}

import java.time.Duration
import java.time.Instant.now
import scala.language.postfixOps

private class LostZombieEventFinder(transactor:       DbTransactor[IO, EventLogDB],
                                    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
)(implicit contextShift:                              ContextShift[IO])
    extends DbClient(Some(queriesExecTimes))
    with EventFinder[IO, ZombieEvent]
    with ZombieEventSubProcess
    with TypeSerializers {

  import doobie.implicits._

  override def popEvent(): IO[Option[ZombieEvent]] = (findEvent >>= markEventTaken) transact transactor.get

  private val maxDurationForEvent = EventProcessingTime(Duration.ofMinutes(5))

  private def findEvent = measureExecutionTime {
    SqlQuery(
      sql"""|SELECT evt.event_id, evt.project_id, proj.project_path, evt.status
            |FROM event evt
            |JOIN project proj ON evt.project_id = proj.project_id
            |WHERE (evt.status = ${GeneratingTriples: EventStatus}
            |  OR evt.status = ${TransformingTriples: EventStatus})
            |  AND evt.message = $zombieMessage
            |  AND  ((${now()} - evt.execution_date) > $maxDurationForEvent)
            |LIMIT 1
    """.stripMargin
        .query[(CompoundEventId, projects.Path, EventStatus)]
        .map { case (id, path, status) => ZombieEvent(processName, id, path, status) }
        .option,
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lze - find event")
    )
  }

  private def markEventTaken: Option[ZombieEvent] => Free[ConnectionOp, Option[ZombieEvent]] = {
    case None        => Free.pure[ConnectionOp, Option[ZombieEvent]](None)
    case Some(event) => updateExecutionDate(event.eventId) map toNoneIfEventAlreadyTaken(event)
  }

  private def toNoneIfEventAlreadyTaken(event: ZombieEvent): Int => Option[ZombieEvent] = {
    case 0 => None
    case 1 => Some(event)
  }

  private def updateExecutionDate(eventId: CompoundEventId) = measureExecutionTime {
    SqlQuery(
      sql"""|UPDATE event
            |SET execution_date = ${now()}
            |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId} AND message = $zombieMessage
            |""".stripMargin.update.run,
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lze - update execution date")
    )
  }

  override val processName: ZombieEventProcess = ZombieEventProcess("lze")
}

private object LostZombieEventFinder {
  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift: ContextShift[IO]): IO[EventFinder[IO, ZombieEvent]] = IO {
    new LostZombieEventFinder(transactor, queriesExecTimes)
  }
}
