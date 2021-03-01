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

import cats.effect.{Bracket, ContextShift, IO}
import cats.free.Free
import cats.syntax.all._
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, TransformingTriples}
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import doobie.free.connection.ConnectionOp
import eu.timepit.refined.api.Refined
import io.renku.eventlog.subscriptions.EventFinder
import io.renku.eventlog.{EventLogDB, TypeSerializers}

private class LostSubscriberEventFinder(transactor:       DbTransactor[IO, EventLogDB],
                                        queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
)(implicit ME:                                            Bracket[IO, Throwable], contextShift: ContextShift[IO])
    extends DbClient(Some(queriesExecTimes))
    with EventFinder[IO, ZombieEvent]
    with TypeSerializers {

  import doobie.implicits._

  override def popEvent(): IO[Option[ZombieEvent]] = (findEvents >>= markEventTaken) transact transactor.get

  private def findEvents = measureExecutionTime {
    SqlQuery(
      sql"""|SELECT DISTINCT evt.event_id, evt.project_id, proj.project_path, evt.status
            |FROM event_delivery delivery
            |JOIN event evt ON evt.event_id = delivery.event_id
            |  AND evt.project_id = delivery.project_id
            |  AND (evt.status = ${GeneratingTriples: EventStatus} OR evt.status = ${TransformingTriples: EventStatus})
            |  AND (evt.message IS NULL OR evt.message <> $zombieMessage)
            |JOIN project proj ON evt.project_id = proj.project_id
            |WHERE NOT EXISTS ( SELECT sub.delivery_url FROM subscriber sub WHERE sub.delivery_url = delivery.delivery_url)
            |LIMIT 1
    """.stripMargin
        .query[(CompoundEventId, projects.Path, EventStatus)]
        .option
        .map(_.map(ZombieEvent.tupled.apply _)),
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lse - find events")
    )
  }

  private def markEventTaken: Option[ZombieEvent] => Free[ConnectionOp, Option[ZombieEvent]] = {
    case None        => Free.pure[ConnectionOp, Option[ZombieEvent]](None)
    case Some(event) => updateMessage(event.eventId) map toNoneIfEventAlreadyTaken(event)
  }

  private def updateMessage(eventId: CompoundEventId) = measureExecutionTime {
    SqlQuery(
      sql"""|UPDATE event
            |SET message = $zombieMessage
            |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId}
            |""".stripMargin.update.run,
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lse - update message")
    )
  }

  private def toNoneIfEventAlreadyTaken(event: ZombieEvent): Int => Option[ZombieEvent] = {
    case 0 => None
    case 1 => Some(event)
  }
}

private object LostSubscriberEventFinder {
  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift: ContextShift[IO]): IO[EventFinder[IO, ZombieEvent]] = IO {
    new LostSubscriberEventFinder(transactor, queriesExecTimes)
  }
}
