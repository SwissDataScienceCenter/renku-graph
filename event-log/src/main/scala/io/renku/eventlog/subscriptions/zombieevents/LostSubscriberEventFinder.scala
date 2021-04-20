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
import cats.free.Free
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, TransformingTriples}
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.api.Refined
import io.renku.eventlog.subscriptions.EventFinder
import io.renku.eventlog.{EventLogDB, TypeSerializers}
import skunk._
import skunk.implicits._
import skunk.codec.all._
import skunk.data.Completion

import java.time.Instant.now
import java.time.{OffsetDateTime, ZoneId}

private class LostSubscriberEventFinder[Interpretation[_]: Async: Bracket[*[_], Throwable]: ContextShift](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name]
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[Interpretation, ZombieEvent]
    with ZombieEventSubProcess
    with TypeSerializers {

  override def popEvent(): Interpretation[Option[ZombieEvent]] = sessionResource.useK {
    findEvents >>= markEventTaken()

  }

  private lazy val findEvents = measureExecutionTimeK {
    SqlQuery(
      Kleisli { session =>
        val query: Query[EventStatus ~ EventStatus ~ String, ZombieEvent] =
          sql"""SELECT DISTINCT evt.event_id, evt.project_id, proj.project_path, evt.status
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
            """.query(eventIdDecoder ~ projectIdDecoder ~ projectPathDecoder ~ eventStatusDecoder).map {
            case eventId ~ projectId ~ projectPath ~ status =>
              ZombieEvent(processName, CompoundEventId(eventId, projectId), projectPath, status)
          }
        session.prepare(query).use(_.option(GeneratingTriples ~ TransformingTriples ~ zombieMessage))
      },
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lse - find events")
    )
  }

  private def markEventTaken()
      : Option[ZombieEvent] => Kleisli[Interpretation, Session[Interpretation], Option[ZombieEvent]] = {
    case None        => Kleisli.pure(Option.empty[ZombieEvent])
    case Some(event) => updateMessage(event.eventId) map toNoneIfEventAlreadyTaken(event)
  }

  private def updateMessage(eventId: CompoundEventId) =
    measureExecutionTimeK {
      SqlQuery(
        Kleisli { session =>
          val query: Command[String ~ OffsetDateTime ~ EventId ~ projects.Id] =
            sql"""UPDATE event
                  SET message = $text, execution_date = $timestamptz
                  WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
                  """.command
          session
            .prepare(query)
            .use(
              _.execute(
                zombieMessage ~ OffsetDateTime.ofInstant(now(), ZoneId.systemDefault()) ~ eventId.id ~ eventId.projectId
              )
            )
        },
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lse - update message")
      )
    }

  private def toNoneIfEventAlreadyTaken(event: ZombieEvent): Completion => Option[ZombieEvent] = {
    case Completion.Update(0) => None
    case Completion.Update(1) => Some(event)
    case completion =>
      throw new Exception(
        s"${categoryName.value.toLowerCase} - lse - Query failed with status $completion"
      ) // TODO verify
  }

  override val processName: ZombieEventProcess = ZombieEventProcess("lse")
}

private object LostSubscriberEventFinder {
  def apply(
      sessionResource:     SessionResource[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift: ContextShift[IO]): IO[EventFinder[IO, ZombieEvent]] = IO {
    new LostSubscriberEventFinder[IO](sessionResource, queriesExecTimes)
  }
}
