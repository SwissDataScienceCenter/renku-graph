/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.commitsyncrequest

import cats.MonadThrow
import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.events.producers.{SubscriptionTypeSerializers, commitsync}
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.CategoryName
import io.renku.graph.model.events.EventDate
import io.renku.graph.model.projects
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

private trait CommitSyncForcer[F[_]] {
  def forceCommitSync(projectId: projects.GitLabId, projectPath: projects.Path): F[Unit]
}

private class CommitSyncForcerImpl[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes]
    extends DbClient(Some(QueriesExecutionTimes[F]))
    with CommitSyncForcer[F]
    with TypeSerializers
    with SubscriptionTypeSerializers {

  override def forceCommitSync(projectId: projects.GitLabId, projectPath: projects.Path): F[Unit] =
    SessionResource[F].useK {
      deleteLastSyncedDate(projectId) flatMap {
        case true  => upsertProject(projectId, projectPath).void
        case false => Kleisli.pure(())
      }
    }

  private def deleteLastSyncedDate(projectId: projects.GitLabId) = measureExecutionTime {
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - delete last_synced"))
      .command[projects.GitLabId ~ CategoryName](sql"""
            DELETE FROM subscription_category_sync_time
            WHERE project_id = $projectIdEncoder AND category_name = $categoryNameEncoder
          """.command)
      .arguments(projectId ~ commitsync.categoryName)
      .build
      .mapResult {
        case Completion.Delete(0) => true
        case _                    => false
      }
  }

  private def upsertProject(projectId: projects.GitLabId, projectPath: projects.Path) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - insert project")
      .command[projects.GitLabId ~ projects.Path ~ EventDate](sql"""
        INSERT INTO project (project_id, project_path, latest_event_date)
        VALUES ($projectIdEncoder, $projectPathEncoder, $eventDateEncoder)
        ON CONFLICT (project_id)
        DO NOTHING
        """.command)
      .arguments(projectId ~ projectPath ~ EventDate(Instant.EPOCH))
      .build
  }
}

private object CommitSyncForcer {
  def apply[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes]: F[CommitSyncForcer[F]] =
    MonadThrow[F].catchNonFatal(new CommitSyncForcerImpl[F])
}
