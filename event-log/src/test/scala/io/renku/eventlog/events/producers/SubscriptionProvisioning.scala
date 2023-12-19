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

package io.renku.eventlog.events.producers

import cats.effect.IO
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.{EventLogDB, EventLogDBProvisioning, EventLogPostgresSpec}
import io.renku.events.CategoryName
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.lastSyncedDates
import io.renku.graph.model.events.LastSyncedDate
import io.renku.graph.model.projects
import skunk._
import skunk.codec.all.varchar
import skunk.implicits._

trait SubscriptionProvisioning extends EventLogDBProvisioning with SubscriptionTypeSerializers {
  self: EventLogPostgresSpec =>

  protected def upsertCategorySyncTime(projectId:    projects.GitLabId,
                                       categoryName: CategoryName,
                                       lastSynced:   LastSyncedDate = lastSyncedDates.generateOne
  )(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Command[projects.GitLabId *: CategoryName *: LastSyncedDate *: EmptyTuple] = sql"""
        INSERT INTO subscription_category_sync_time (project_id, category_name, last_synced)
        VALUES ($projectIdEncoder, $categoryNameEncoder, $lastSyncedDateEncoder)
        ON CONFLICT (project_id, category_name)
        DO UPDATE SET  last_synced = excluded.last_synced
      """.command
      session.prepare(query).flatMap(_.execute(projectId *: categoryName *: lastSynced *: EmptyTuple)).void
    }

  protected def findSyncTime(projectId: projects.GitLabId, categoryName: CategoryName)(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[Option[LastSyncedDate]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[projects.GitLabId *: CategoryName *: EmptyTuple, LastSyncedDate] = sql"""
        SELECT last_synced
        FROM subscription_category_sync_time
        WHERE project_id = $projectIdEncoder AND category_name = $categoryNameEncoder
      """.query(lastSyncedDateDecoder)
      session.prepare(query).flatMap(_.option(projectId *: categoryName *: EmptyTuple))
    }

  protected case class CategorySync(name: CategoryName, lastSyncedDate: LastSyncedDate)

  protected def findCategorySyncTimes(
      projectId: projects.GitLabId
  )(implicit cfg: DBConfig[EventLogDB]): IO[List[CategorySync]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[projects.GitLabId, CategorySync] = sql"""
          SELECT category_name, last_synced
          FROM subscription_category_sync_time
          WHERE project_id = $projectIdEncoder"""
        .query(varchar ~ lastSyncedDateDecoder)
        .map { case (category: String) ~ (lastSynced: LastSyncedDate) =>
          CategorySync(CategoryName(category), lastSynced)
        }
      session.prepare(query).flatMap(_.stream(projectId, 32).compile.toList)
    }
}
