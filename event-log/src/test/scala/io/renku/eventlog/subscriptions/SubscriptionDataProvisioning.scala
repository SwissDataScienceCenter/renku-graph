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

package io.renku.eventlog.subscriptions

import cats.data.Kleisli
import ch.datascience.graph.model.events.{CategoryName, LastSyncedDate}
import ch.datascience.graph.model.projects
import io.renku.eventlog.{EventLogDataProvisioning, InMemoryEventLogDb}
import skunk._
import skunk.implicits._

trait SubscriptionDataProvisioning extends EventLogDataProvisioning with SubscriptionTypeSerializers {
  self: InMemoryEventLogDb =>

  protected def upsertLastSynced(projectId: projects.Id, categoryName: CategoryName, lastSynced: LastSyncedDate): Unit =
    execute[Unit] {
      Kleisli { session =>
        val query: Command[projects.Id ~ CategoryName ~ LastSyncedDate] = sql"""
        INSERT INTO
        subscription_category_sync_time (project_id, category_name, last_synced)
        VALUES ($projectIdEncoder, $categoryNameEncoder, $lastSyncedDateEncoder)
        ON CONFLICT (project_id, category_name)
        DO UPDATE SET  last_synced = excluded.last_synced
      """.command
        session.prepare(query).use(_.execute(projectId ~ categoryName ~ lastSynced)).void
      }
    }

  protected def findSyncTime(projectId: projects.Id, categoryName: CategoryName): Option[LastSyncedDate] =
    execute {
      Kleisli { session =>
        val query: Query[projects.Id ~ CategoryName, LastSyncedDate] = sql"""
        SELECT last_synced
        FROM subscription_category_sync_time
        WHERE project_id = $projectIdEncoder AND category_name = $categoryNameEncoder
      """.query(lastSyncedDateDecoder)
        session.prepare(query).use(_.option(projectId ~ categoryName))
      }
    }
}
