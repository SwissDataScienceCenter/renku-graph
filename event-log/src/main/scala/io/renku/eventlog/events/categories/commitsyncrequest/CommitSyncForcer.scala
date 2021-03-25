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

import cats.effect.{Bracket, IO}
import cats.free.Free
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import doobie.free.connection.ConnectionOp
import eu.timepit.refined.api.Refined
import io.renku.eventlog.subscriptions.commitsync
import io.renku.eventlog.{EventLogDB, TypeSerializers}

import java.time.Instant

private trait CommitSyncForcer[Interpretation[_]] {
  def forceCommitSync(projectId: projects.Id, projectPath: projects.Path): Interpretation[Unit]
}

private class CommitSyncForcerImpl(
    transactor:       DbTransactor[IO, EventLogDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
    now:              () => Instant = () => Instant.now
)(implicit ME:        Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with CommitSyncForcer[IO]
    with TypeSerializers {

  import doobie.implicits._

  override def forceCommitSync(projectId: projects.Id, projectPath: projects.Path): IO[Unit] = {
    deleteLastSyncedDate(projectId) flatMap {
      case 0 => upsertProject(projectId, projectPath)
      case _ => Free.pure[ConnectionOp, Int](1)
    } transact transactor.get
  }.void

  private def deleteLastSyncedDate(projectId: projects.Id) = measureExecutionTime {
    SqlQuery(
      sql"""|DELETE FROM subscription_category_sync_time
            |WHERE project_id = $projectId AND category_name = ${commitsync.categoryName}
            |""".stripMargin.update.run,
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - delete last_synced")
    )
  }

  private def upsertProject(projectId: projects.Id, projectPath: projects.Path) = measureExecutionTime {
    SqlQuery(
      sql"""|INSERT INTO
            |project (project_id, project_path, latest_event_date)
            |VALUES ($projectId, $projectPath, ${now()})
            |ON CONFLICT (project_id)
            |DO NOTHING
      """.stripMargin.update.run,
      name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - insert project")
    )
  }
}

private object CommitSyncForcer {
  import cats.effect.IO

  def apply(transactor:       DbTransactor[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
  ): IO[CommitSyncForcer[IO]] = IO {
    new CommitSyncForcerImpl(transactor, queriesExecTimes)
  }
}
