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

package io.renku.eventlog.subscriptions.minprojectinfo

import cats.MonadThrow
import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.subscriptions.{EventFinder, SubscriptionTypeSerializers}
import io.renku.events.CategoryName
import io.renku.graph.model.events.EventStatus.TriplesStore
import io.renku.graph.model.events.LastSyncedDate
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import skunk.data.Completion

import java.time.Instant

private class EventFinderImpl[F[_]: MonadCancelThrow: SessionResource](queriesExecTimes: LabeledHistogram[F],
                                                                       now: () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[F, MinProjectInfoEvent]
    with SubscriptionTypeSerializers {

  import skunk._
  import skunk.implicits._

  override def popEvent(): F[Option[MinProjectInfoEvent]] = SessionResource[F].useK {
    findEvent >>= {
      case None        => Kleisli.pure(Option.empty[MinProjectInfoEvent])
      case Some(event) => markTaken(event.projectId) map toNoneIfEventAlreadyTaken(event)
    }
  }

  private def findEvent = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.show.toLowerCase} - find event")
      .select[Void, MinProjectInfoEvent](
        sql"""SELECT p.project_id, p.project_path
              FROM project p
              WHERE NOT EXISTS (
                SELECT project_id 
                FROM subscription_category_sync_time st 
                WHERE st.category_name = '#${categoryName.show}' 
                  AND st.project_id = p.project_id
              ) AND NOT EXISTS (
                SELECT event_id 
                FROM event e
                WHERE e.project_id = p.project_id
                  AND e.status = '#${TriplesStore.value}'
              )
              LIMIT 1
      """.query(projectIdDecoder ~ projectPathDecoder)
          .map { case (id: projects.Id) ~ (path: projects.Path) => MinProjectInfoEvent(id, path) }
      )
      .arguments(Void)
      .build(_.option)
  }

  private def markTaken(projectId: projects.Id): Kleisli[F, Session[F], Boolean] = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - insert last_synced")
      .command[projects.Id ~ CategoryName ~ LastSyncedDate](sql"""
        INSERT INTO subscription_category_sync_time(project_id, category_name, last_synced)
        VALUES ($projectIdEncoder, $categoryNameEncoder, $lastSyncedDateEncoder)
        ON CONFLICT (project_id, category_name)
        DO UPDATE SET last_synced = EXCLUDED.last_synced
        """.command)
      .arguments(projectId ~ categoryName ~ LastSyncedDate(now()))
      .build
      .flatMapResult {
        case Completion.Insert(1) => true.pure[F]
        case Completion.Insert(0) => false.pure[F]
        case completion =>
          new Exception(s"${categoryName.show}: insert last_synced failed with completion code $completion")
            .raiseError[F, Boolean]
      }
  }

  private def toNoneIfEventAlreadyTaken(event: MinProjectInfoEvent): Boolean => Option[MinProjectInfoEvent] = {
    case true  => Some(event)
    case false => None
  }
}

private object EventFinder {
  def apply[F[_]: MonadCancelThrow: SessionResource](
      queriesExecTimes: LabeledHistogram[F]
  ): F[EventFinder[F, MinProjectInfoEvent]] = MonadThrow[F].catchNonFatal {
    new EventFinderImpl(queriesExecTimes)
  }
}
