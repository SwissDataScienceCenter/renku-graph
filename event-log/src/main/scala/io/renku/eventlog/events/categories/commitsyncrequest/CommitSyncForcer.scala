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

package io.renku.eventlog.events.categories.commitsyncrequest

import cats.data.Kleisli
import cats.effect.BracketThrow
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlStatement}
import ch.datascience.graph.model.events.CategoryName
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.api.Refined
import io.renku.eventlog.subscriptions.{SubscriptionTypeSerializers, commitsync}
import io.renku.eventlog.{EventDate, EventLogDB, TypeSerializers}
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

private trait CommitSyncForcer[Interpretation[_]] {
  def forceCommitSync(projectId: projects.Id, projectPath: projects.Path): Interpretation[Unit]
}

private class CommitSyncForcerImpl[Interpretation[_]: BracketThrow](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with CommitSyncForcer[Interpretation]
    with TypeSerializers
    with SubscriptionTypeSerializers {

  override def forceCommitSync(projectId: projects.Id, projectPath: projects.Path): Interpretation[Unit] =
    sessionResource.useK {
      deleteLastSyncedDate(projectId) flatMap {
        case true  => upsertProject(projectId, projectPath).void
        case false => Kleisli.pure(())
      }
    }

  private def deleteLastSyncedDate(projectId: projects.Id) = measureExecutionTime {
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - delete last_synced"))
      .command[projects.Id ~ CategoryName](sql"""
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

  private def upsertProject(projectId: projects.Id, projectPath: projects.Path) = measureExecutionTime {
    SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - insert project"))
      .command[projects.Id ~ projects.Path ~ EventDate](sql"""
          INSERT INTO
          project (project_id, project_path, latest_event_date)
          VALUES ($projectIdEncoder, $projectPathEncoder, $eventDateEncoder)
          ON CONFLICT (project_id)
          DO NOTHING
      """.command)
      .arguments(projectId ~ projectPath ~ EventDate(now()))
      .build
  }
}

private object CommitSyncForcer {

  import cats.effect.IO

  def apply(sessionResource:  SessionResource[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlStatement.Name]
  ): IO[CommitSyncForcer[IO]] = IO {
    new CommitSyncForcerImpl(sessionResource, queriesExecTimes)
  }
}
