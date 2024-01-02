/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.producers
package globalcommitsync

import cats.MonadThrow
import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.CategoryName
import io.renku.graph.model.events.LastSyncedDate
import io.renku.graph.model.projects
import skunk._
import skunk.data.Completion
import skunk.implicits._

private trait LastSyncedDateUpdater[F[_]] {
  def run(projectId: projects.GitLabId, maybeLastSyncDate: Option[LastSyncedDate]): F[Completion]
}

private object LastSyncedDateUpdater {
  def apply[F[_]: Async: SessionResource: QueriesExecutionTimes]: F[LastSyncedDateUpdater[F]] =
    MonadThrow[F].catchNonFatal(
      new LastSyncedDateUpdateImpl[F]
    )
}

private class LastSyncedDateUpdateImpl[F[_]: Async: SessionResource: QueriesExecutionTimes]
    extends DbClient(Some(QueriesExecutionTimes[F]))
    with LastSyncedDateUpdater[F]
    with SubscriptionTypeSerializers {

  override def run(projectId: projects.GitLabId, maybeLastSyncDate: Option[LastSyncedDate]): F[Completion] =
    SessionResource[F].useK {
      maybeLastSyncDate match {
        case Some(lastSyncedDate) => insert(projectId, lastSyncedDate)
        case None                 => deleteLastSyncDate(projectId)
      }
    }

  private def insert(projectId: projects.GitLabId, lastSyncedDate: LastSyncedDate) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - update last_synced")
      .command[projects.GitLabId *: CategoryName *: LastSyncedDate *: EmptyTuple](sql"""
        INSERT INTO subscription_category_sync_time(project_id, category_name, last_synced)
        VALUES ($projectIdEncoder, $categoryNameEncoder, $lastSyncedDateEncoder)
        ON CONFLICT (project_id, category_name)
        DO UPDATE SET last_synced = EXCLUDED.last_synced
        """.command)
      .arguments(projectId *: categoryName *: lastSyncedDate *: EmptyTuple)
      .build
  } recoverWith { case SqlState.ForeignKeyViolation(_) => Kleisli.pure(Completion.Insert(0)) }

  private def deleteLastSyncDate(projectId: projects.GitLabId) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - delete last_synced")
      .command[projects.GitLabId *: CategoryName *: EmptyTuple](sql"""
        DELETE FROM subscription_category_sync_time
        WHERE project_id = $projectIdEncoder AND category_name = $categoryNameEncoder
        """.command)
      .arguments(projectId *: categoryName *: EmptyTuple)
      .build
  }
}
