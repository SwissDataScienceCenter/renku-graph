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

package io.renku.eventlog.subscriptions.globalcommitsync

import cats.effect.{BracketThrow, IO}
import ch.datascience.db.{DbClient, SessionResource, SqlStatement}
import ch.datascience.graph.model.events.{CategoryName, LastSyncedDate}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.api.Refined
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.subscriptions.SubscriptionTypeSerializers
import skunk.data.Completion
import skunk.implicits.{toIdOps, toStringOps}
import skunk.~

private trait LastSyncedDateUpdater[Interpretation[_]] {
  def run(projectId: projects.Id, maybeLastSyncDate: Option[LastSyncedDate]): Interpretation[Completion]
}

private object LastSyncedDateUpdater {
  def apply(sessionResource:  SessionResource[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlStatement.Name]
  ): IO[LastSyncedDateUpdater[IO]] = IO(
    new LastSyncedDateUpdateImpl[IO](sessionResource, queriesExecTimes)
  )
}

private class LastSyncedDateUpdateImpl[Interpretation[_]: BracketThrow](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name]
) extends DbClient(Some(queriesExecTimes))
    with LastSyncedDateUpdater[Interpretation]
    with SubscriptionTypeSerializers {

  override def run(projectId: projects.Id, maybeLastSyncDate: Option[LastSyncedDate]): Interpretation[Completion] =
    sessionResource.useK(measureExecutionTime {
      maybeLastSyncDate match {
        case Some(lastSyncedDate) =>
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
        case None =>
          SqlStatement(name = Refined.unsafeApply(s"${categoryName.value.toLowerCase} - delete last_synced"))
            .command[projects.Id ~ CategoryName](
              sql"""DELETE  FROM subscription_category_sync_time
                WHERE project_id = $projectIdEncoder AND category_name = $categoryNameEncoder
            """.command
            )
            .arguments(projectId ~ categoryName)
            .build
      }
    })
}
