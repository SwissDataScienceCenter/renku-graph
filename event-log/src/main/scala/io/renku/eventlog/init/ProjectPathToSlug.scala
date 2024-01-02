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

package io.renku.eventlog.init

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import org.typelevel.log4cats.Logger
import skunk._
import skunk.implicits._

private trait ProjectPathToSlug[F[_]] extends DbMigrator[F]

private object ProjectPathToSlug {
  def apply[F[_]: MonadCancelThrow: Logger: SessionResource]: ProjectPathToSlug[F] = new ProjectPathToSlugImpl[F]
}

private class ProjectPathToSlugImpl[F[_]: MonadCancelThrow: Logger: SessionResource] extends ProjectPathToSlug[F] {
  import MigratorTools._

  override def run: F[Unit] = SessionResource[F].useK {
    checkAndRenameColumnIfNeeded(table = "project") >>
      checkAndRenameColumnIfNeeded(table = "clean_up_events_queue") >>
      removePathIndex() >>
      checkAndCreateSlugIndexIfAbsent(table = "project") >>
      checkAndCreateSlugIndexIfAbsent(table = "clean_up_events_queue")
  }

  private def checkAndRenameColumnIfNeeded(table: String) =
    checkColumnExists(table, "project_slug") >>= {
      case true  => Kleisli.liftF(Logger[F].info(s"column '$table.project_slug' existed"))
      case false => renameColumn(table)
    }

  private def renameColumn(table: String): Kleisli[F, Session[F], Unit] =
    execute(sql"ALTER TABLE #$table RENAME COLUMN project_path TO project_slug".command)
      .flatMapF(_ => Logger[F].info(s"column '$table.project_path' renamed to '$table.project_slug'"))

  private def removePathIndex(): Kleisli[F, Session[F], Unit] = {
    val idx = "idx_project_path"
    checkIndexExists(idx) >>= {
      case false => Kleisli.liftF(Logger[F].info(s"index '$idx' not existed"))
      case true =>
        execute(sql"DROP INDEX IF EXISTS #$idx".command)
          .flatMapF(_ => Logger[F].info(s"index '$idx' removed"))
    }
  }

  private def checkAndCreateSlugIndexIfAbsent(table: String) = {
    val idx = s"idx_${table}_project_slug"
    checkIndexExists(table, idx) >>= {
      case true  => Kleisli.liftF(Logger[F].info(s"index '$idx' existed"))
      case false => createIndex(table, idx)
    }
  }

  private def createIndex(table: String, idx: String): Kleisli[F, Session[F], Unit] =
    execute(sql"CREATE INDEX IF NOT EXISTS #$idx ON #$table(project_slug)".command)
      .flatMapF(_ => Logger[F].info(s"index '$idx' created"))
}
