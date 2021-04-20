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

package io.renku.eventlog.subscriptions.membersync

import cats.data.Kleisli
import cats.effect.{Async, Bracket, ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.events.{CategoryName, LastSyncedDate}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.api.Refined
import io.renku.eventlog.{EventDate, EventLogDB}
import io.renku.eventlog.subscriptions.{EventFinder, SubscriptionTypeSerializers}
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

private class MemberSyncEventFinderImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]: ContextShift](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[Interpretation, MemberSyncEvent]
    with SubscriptionTypeSerializers {

  override def popEvent(): Interpretation[Option[MemberSyncEvent]] = sessionResource.useK {
    findEventAndMarkTaken()
  }

  private def findEventAndMarkTaken() =
    findEvent >>= {
      case Some((projectId, maybeSyncedDate, event)) =>
        setSyncDate(projectId, maybeSyncedDate) map toNoneIfEventAlreadyTaken(event)
      case None => Kleisli.pure(Option.empty[MemberSyncEvent])
    }

  private lazy val findEvent = measureExecutionTimeK {
    SqlQuery[Interpretation, Option[(projects.Id, Option[LastSyncedDate], MemberSyncEvent)]](
      Kleisli { session =>
        val query
            : Query[CategoryName ~ EventDate ~ LastSyncedDate ~ EventDate ~ LastSyncedDate ~ EventDate ~ LastSyncedDate,
                    (projects.Id, Option[LastSyncedDate], MemberSyncEvent)
            ] =
          sql"""SELECT proj.project_id, sync_time.last_synced, proj.project_path
                  FROM project proj
                  LEFT JOIN subscription_category_sync_time sync_time
                    ON sync_time.project_id = proj.project_id AND sync_time.category_name = $categoryNamePut
                  WHERE
                    sync_time.last_synced IS NULL
                    OR (
                         (($eventDatePut - proj.latest_event_date) < INTERVAL '1 hour' AND ($lastSyncedDatePut - sync_time.last_synced) > INTERVAL '1 minute')
                      OR (($eventDatePut - proj.latest_event_date) < INTERVAL '1 day'  AND ($lastSyncedDatePut - sync_time.last_synced) > INTERVAL '1 hour')
                      OR (($eventDatePut - proj.latest_event_date) > INTERVAL '1 day'  AND ($lastSyncedDatePut - sync_time.last_synced) > INTERVAL '1 day')
                    )
                  ORDER BY proj.latest_event_date DESC
                  LIMIT 1
      """.query(projectIdGet ~ lastSyncedDateGet.opt ~ projectPathGet)
            .map { case id ~ maybeDate ~ path => (id, maybeDate, MemberSyncEvent(path)) }
        val eventDate    = EventDate(now())
        val lastSyncDate = LastSyncedDate(now())
        session
          .prepare(query)
          .use(_.option(categoryName ~ eventDate ~ lastSyncDate ~ eventDate ~ lastSyncDate ~ eventDate ~ lastSyncDate))

      },
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - find event")
    )
  }

  private def setSyncDate(projectId: projects.Id, maybeSyncedDate: Option[LastSyncedDate]) =
    if (maybeSyncedDate.isDefined) updateLastSyncedDate(projectId)
    else insertLastSyncedDate(projectId)

  private def updateLastSyncedDate(projectId: projects.Id) =
    measureExecutionTimeK {
      SqlQuery(
        Kleisli { session =>
          val query: Command[LastSyncedDate ~ projects.Id ~ CategoryName] =
            sql"""
            UPDATE subscription_category_sync_time
            SET last_synced = $lastSyncedDatePut
            WHERE project_id = $projectIdPut AND category_name = $categoryNamePut
            """.command
          session.prepare(query).use(_.execute(LastSyncedDate(now()) ~ projectId ~ categoryName)).map {
            case Completion.Update(n) => n
            case _                    => 0 // TODO verify if the default value is relevant or if we should throw an error
          }
        },
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - update last_synced")
      )
    }

  private def insertLastSyncedDate(projectId: projects.Id) =
    measureExecutionTimeK {
      SqlQuery(
        Kleisli { session =>
          val query: Command[projects.Id ~ CategoryName ~ LastSyncedDate] =
            sql"""
            INSERT INTO subscription_category_sync_time(project_id, category_name, last_synced)
            VALUES ($projectIdPut, $categoryNamePut, $lastSyncedDatePut)
            ON CONFLICT (project_id, category_name)
            DO
              UPDATE SET last_synced = EXCLUDED.last_synced
            """.command
          session.prepare(query).use(_.execute(projectId ~ categoryName ~ LastSyncedDate(now()))).map {
            case Completion.Insert(n) => n
            case _                    => 0 // TODO verify if the default value is relevant or if we should throw an error
          }
        },
        name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - insert last_synced")
      )
    }

  private def toNoneIfEventAlreadyTaken(event: MemberSyncEvent): Int => Option[MemberSyncEvent] = {
    case 0 => None
    case 1 => Some(event)
  }
}

private object MemberSyncEventFinder {
  def apply(
      sessionResource:  SessionResource[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
  )(implicit ME:        Bracket[IO, Throwable], contextShift: ContextShift[IO]): IO[EventFinder[IO, MemberSyncEvent]] = IO {
    new MemberSyncEventFinderImpl(sessionResource, queriesExecTimes)
  }
}
