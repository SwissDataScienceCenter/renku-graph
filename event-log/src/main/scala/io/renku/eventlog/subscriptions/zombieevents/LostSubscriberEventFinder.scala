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
import doobie.free.connection.{ConnectionOp, raiseError}
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
    transactor:       SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name]
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[Interpretation, ZombieEvent]
    with ZombieEventSubProcess
    with TypeSerializers {

  override def popEvent(): Interpretation[Option[ZombieEvent]] = transactor.use { implicit session =>
    session.transaction.use { transation =>
      for {
        sp <- transation.savepoint
        result <- (findEvents >>= markEventTaken) recoverWith { error =>
                    transation.rollback(sp).flatMap(_ => error.raiseError[Interpretation, Option[ZombieEvent]])
                  }
      } yield result
    }
  }

  private def findEvents(implicit session: Session[Interpretation]) = measureExecutionTime {
    SqlQuery(
      Kleisli { session =>
        val query: Query[EventStatus ~ EventStatus ~ String, ZombieEvent] =
          sql"""SELECT DISTINCT evt.event_id, evt.project_id, proj.project_path, evt.status
                FROM event_delivery delivery
                JOIN event evt ON evt.event_id = delivery.event_id
                  AND evt.project_id = delivery.project_id
                  AND (evt.status = $eventStatusPut OR evt.status = $eventStatusPut)
                  AND (evt.message IS NULL OR evt.message <> $text)
                JOIN project proj ON evt.project_id = proj.project_id
                WHERE NOT EXISTS ( 
                  SELECT sub.delivery_id 
                  FROM subscriber sub 
                  WHERE sub.delivery_id = delivery.delivery_id
                )
                LIMIT 1
            """.query(eventIdGet ~ projectIdGet ~ projectPathGet ~ eventStatusGet).map {
            case eventId ~ projectId ~ projectPath ~ status =>
              ZombieEvent(processName, CompoundEventId(eventId, projectId), projectPath, status)
          }
        session.prepare(query).use(_.option(GeneratingTriples ~ TransformingTriples ~ zombieMessage))
      },
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - lse - find events")
    )
  }

  private def markEventTaken(implicit
      session: Session[Interpretation]
  ): Option[ZombieEvent] => Interpretation[Option[ZombieEvent]] = {
    case None        => Option.empty[ZombieEvent].pure[Interpretation]
    case Some(event) => updateMessage(event.eventId) map toNoneIfEventAlreadyTaken(event)
  }

  private def updateMessage(eventId: CompoundEventId)(implicit session: Session[Interpretation]) =
    measureExecutionTime {
      SqlQuery(
        Kleisli { session =>
          val query: Command[String ~ OffsetDateTime ~ EventId ~ projects.Id] =
            sql"""UPDATE event
                  SET message = $text, execution_date = $timestamptz
                  WHERE event_id = $eventIdPut AND project_id = $projectIdPut
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
      transactor:          SessionResource[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift: ContextShift[IO]): IO[EventFinder[IO, ZombieEvent]] = IO {
    new LostSubscriberEventFinder[IO](transactor, queriesExecTimes)
  }
}
