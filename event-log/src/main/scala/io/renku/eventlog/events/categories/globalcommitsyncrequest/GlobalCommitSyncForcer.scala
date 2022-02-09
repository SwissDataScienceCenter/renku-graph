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

package io.renku.eventlog.events.categories.globalcommitsyncrequest

import cats.MonadThrow
import cats.effect.MonadCancelThrow
import eu.timepit.refined.api.Refined
import io.renku.db.{DbClient, SessionResource, SqlStatement}
import io.renku.eventlog.subscriptions.{SubscriptionTypeSerializers, globalcommitsync}
import io.renku.eventlog.{EventLogDB, TypeSerializers}
import io.renku.graph.model.events.CategoryName
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import skunk._
import skunk.implicits._
import cats.data.Kleisli
import io.renku.eventlog.EventDate
import java.time.Instant
import skunk.data.Completion

private trait GlobalCommitSyncForcer[F[_]] {
  def forceGlobalCommitSync(projectId: projects.Id, projectPath: projects.Path): F[Unit]
}

private class GlobalCommitSyncForcerImpl[F[_]: MonadCancelThrow](
    sessionResource:  SessionResource[F, EventLogDB],
    queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
) extends DbClient(Some(queriesExecTimes))
    with GlobalCommitSyncForcer[F]
    with TypeSerializers
    with SubscriptionTypeSerializers {

  override def forceGlobalCommitSync(projectId: projects.Id, projectPath: projects.Path): F[Unit] =
    sessionResource.useK {
      deleteLastSyncedDate(projectId) flatMap {
        case true  => upsertProject(projectId, projectPath)
        case false => Kleisli.pure(())
      }
    }

  private def deleteLastSyncedDate(projectId: projects.Id) = measureExecutionTime {
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - delete last_synced"))
      .command[projects.Id ~ CategoryName](sql"""
            DELETE FROM subscription_category_sync_time
            WHERE project_id = $projectIdEncoder AND category_name = $categoryNameEncoder
          """.command)
      .arguments(projectId ~ globalcommitsync.categoryName)
      .build
      .mapResult {
        case Completion.Delete(0) => true
        case _                    => false
      }
  }

  private def upsertProject(projectId: projects.Id, projectPath: projects.Path) = measureExecutionTime {
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - insert project"))
      .command[projects.Id ~ projects.Path ~ EventDate](sql"""
          INSERT INTO
          project (project_id, project_path, latest_event_date)
          VALUES ($projectIdEncoder, $projectPathEncoder, $eventDateEncoder)
          ON CONFLICT (project_id)
          DO NOTHING
      """.command)
      .arguments(projectId ~ projectPath ~ EventDate(Instant.EPOCH))
      .build
      .void
  }
}

private object GlobalCommitSyncForcer {

  def apply[F[_]: MonadCancelThrow](sessionResource: SessionResource[F, EventLogDB],
                                    queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ): F[GlobalCommitSyncForcer[F]] = MonadThrow[F].catchNonFatal {
    new GlobalCommitSyncForcerImpl(sessionResource, queriesExecTimes)
  }
}
