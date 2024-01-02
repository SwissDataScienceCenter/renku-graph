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

package io.renku.tokenrepository.repository.init

import MigrationTools._
import cats.data.Kleisli
import cats.effect.{Async, Spawn}
import cats.syntax.all._
import io.renku.tokenrepository.repository.ProjectsTokensDB.SessionResource
import io.renku.tokenrepository.repository.TokenRepositoryTypeSerializers
import org.typelevel.log4cats.Logger
import skunk._
import skunk.implicits._

private object ProjectPathToSlug {
  def apply[F[_]: Async: Logger: SessionResource]: DBMigration[F] = new ProjectPathToSlug[F]
}

private class ProjectPathToSlug[F[_]: Spawn: Logger: SessionResource]
    extends DBMigration[F]
    with TokenRepositoryTypeSerializers {

  override def run: F[Unit] = SessionResource[F].useK {
    checkColumnExists("projects_tokens", "project_slug") >>= {
      case true  => Kleisli.liftF(Logger[F].info("'project_slug' column existed"))
      case false => renameColumn()
    }
  }

  private def renameColumn(): Kleisli[F, Session[F], Unit] = Kleisli { implicit session =>
    {
      for {
        _ <- execute(sql"ALTER TABLE projects_tokens RENAME COLUMN project_path TO project_slug".command)
        _ <- execute(sql"ALTER INDEX IF EXISTS idx_project_path RENAME TO idx_project_slug".command)
        _ <- Logger[F].info("column 'project_path' renamed to 'project_slug'")
      } yield ()
    } onError log
  }

  private lazy val log: PartialFunction[Throwable, F[Unit]] =
    Logger[F].error(_)("renaming column 'project_path' to 'project_slug' failed")
}
