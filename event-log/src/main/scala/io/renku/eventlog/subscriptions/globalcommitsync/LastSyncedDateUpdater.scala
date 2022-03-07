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

package io.renku.eventlog.subscriptions.globalcommitsync

import cats.MonadThrow
import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.subscriptions.SubscriptionTypeSerializers
import io.renku.events.CategoryName
import io.renku.graph.model.events.LastSyncedDate
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import skunk.data.Completion
import skunk.implicits.{toIdOps, toStringOps}
import skunk.{SqlState, ~}

private trait LastSyncedDateUpdater[F[_]] {
  def run(projectId: projects.Id, maybeLastSyncDate: Option[LastSyncedDate]): F[Completion]
}

private object LastSyncedDateUpdater {
  def apply[F[_]: MonadCancelThrow: SessionResource](
      queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ): F[LastSyncedDateUpdater[F]] = MonadThrow[F].catchNonFatal(
    new LastSyncedDateUpdateImpl[F](queriesExecTimes)
  )
}

private class LastSyncedDateUpdateImpl[F[_]: MonadCancelThrow: SessionResource](
    queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
) extends DbClient(Some(queriesExecTimes))
    with LastSyncedDateUpdater[F]
    with SubscriptionTypeSerializers {

  override def run(projectId: projects.Id, maybeLastSyncDate: Option[LastSyncedDate]): F[Completion] =
    SessionResource[F].useK {
      maybeLastSyncDate match {
        case Some(lastSyncedDate) => insert(projectId, lastSyncedDate)
        case None                 => deleteLastSyncDate(projectId)
      }
    }

  private def insert(projectId: projects.Id, lastSyncedDate: LastSyncedDate) = measureExecutionTime {
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - update last_synced"))
      .command[projects.Id ~ CategoryName ~ LastSyncedDate](
        sql"""INSERT INTO subscription_category_sync_time(project_id, category_name, last_synced)
              VALUES ( $projectIdEncoder, $categoryNameEncoder, $lastSyncedDateEncoder)
              ON CONFLICT (project_id, category_name)
              DO UPDATE SET last_synced = EXCLUDED.last_synced
            """.command
      )
      .arguments(projectId ~ categoryName ~ lastSyncedDate)
      .build
  } recoverWith { case SqlState.ForeignKeyViolation(_) => Kleisli.pure(Completion.Insert(0)) }

  private def deleteLastSyncDate(projectId: projects.Id) = measureExecutionTime {
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - delete last_synced"))
      .command[projects.Id ~ CategoryName](
        sql"""DELETE FROM subscription_category_sync_time
              WHERE project_id = $projectIdEncoder AND category_name = $categoryNameEncoder
            """.command
      )
      .arguments(projectId ~ categoryName)
      .build
  }
}
