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

import cats.effect.{Bracket, ContextShift, IO}
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.LastSyncedDate
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import doobie.free.connection.ConnectionOp
import eu.timepit.refined.api.Refined
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.subscriptions.{EventFinder, SubscriptionTypeSerializers}

import java.time.Instant

private class MemberSyncEventFinderImpl(transactor:       DbTransactor[IO, EventLogDB],
                                        queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
                                        now:              () => Instant = () => Instant.now
)(implicit ME:                                            Bracket[IO, Throwable], contextShift: ContextShift[IO])
    extends DbClient(Some(queriesExecTimes))
    with EventFinder[IO, MemberSyncEvent]
    with SubscriptionTypeSerializers {

  import cats.free.Free
  import doobie.implicits._

  override def popEvent(): IO[Option[MemberSyncEvent]] = findEventAndMarkTaken() transact transactor.get

  private def findEventAndMarkTaken() =
    findEvent() flatMap {
      case Some((projectId, maybeSyncedDate, event)) =>
        setSyncDate(projectId, maybeSyncedDate) map toNoneIfEventAlreadyTaken(event)
      case None => Free.pure[ConnectionOp, Option[MemberSyncEvent]](None)
    }

  private def findEvent() = measureExecutionTime {
    SqlQuery(
      sql"""|SELECT proj.project_id, sync_time.last_synced, proj.project_path
            |FROM project proj
            |LEFT JOIN subscription_category_sync_time sync_time 
            |  ON sync_time.project_id = proj.project_id AND sync_time.category_name = $categoryName
            |WHERE
            |  sync_time.last_synced IS NULL
            |  OR (
            |       ((${now()} - proj.latest_event_date) < INTERVAL '1 hour' AND (${now()} - sync_time.last_synced) > INTERVAL '1 minute')
            |    OR ((${now()} - proj.latest_event_date) < INTERVAL '1 day'  AND (${now()} - sync_time.last_synced) > INTERVAL '1 hour')
            |    OR ((${now()} - proj.latest_event_date) > INTERVAL '1 day'  AND (${now()} - sync_time.last_synced) > INTERVAL '1 day')
            |  )
            |ORDER BY proj.latest_event_date DESC 
            |LIMIT 1
      """.stripMargin
        .query[(projects.Id, Option[LastSyncedDate], MemberSyncEvent)]
        .option,
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - find event")
    )
  }

  private def setSyncDate(projectId: projects.Id, maybeSyncedDate: Option[LastSyncedDate]) =
    if (maybeSyncedDate.isDefined) updateLastSyncedDate(projectId)
    else insertLastSyncedDate(projectId)

  private def updateLastSyncedDate(projectId: projects.Id) = measureExecutionTime {
    SqlQuery(
      sql"""|UPDATE subscription_category_sync_time 
            |SET last_synced = ${now()}
            |WHERE project_id = $projectId AND category_name = $categoryName
            |""".stripMargin.update.run,
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - update last_synced")
    )
  }

  private def insertLastSyncedDate(projectId: projects.Id) = measureExecutionTime {
    SqlQuery(
      sql"""|INSERT INTO subscription_category_sync_time(project_id, category_name, last_synced)
            |VALUES ($projectId, $categoryName, ${now()})
            |ON CONFLICT (project_id, category_name)
            |DO 
            |  UPDATE SET last_synced = EXCLUDED.last_synced
            |""".stripMargin.update.run,
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
      transactor:       DbTransactor[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
  )(implicit ME:        Bracket[IO, Throwable], contextShift: ContextShift[IO]): IO[EventFinder[IO, MemberSyncEvent]] = IO {
    new MemberSyncEventFinderImpl(transactor, queriesExecTimes)
  }
}
