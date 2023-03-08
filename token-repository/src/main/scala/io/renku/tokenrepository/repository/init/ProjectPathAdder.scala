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

package io.renku.tokenrepository.repository.init

import cats.data.Kleisli
import cats.effect._
import cats.syntax.all._
import io.renku.tokenrepository.repository.ProjectsTokensDB.SessionResource
import io.renku.tokenrepository.repository.TokenRepositoryTypeSerializers
import org.typelevel.log4cats.Logger
import skunk._
import skunk.implicits._

import scala.util.control.NonFatal

private object ProjectPathAdder {
  def apply[F[_]: Async: Logger: SessionResource]: DBMigration[F] = new ProjectPathAdder[F]
}

private class ProjectPathAdder[F[_]: Spawn: Logger: SessionResource]
    extends DBMigration[F]
    with TokenRepositoryTypeSerializers {

  import MigrationTools._

  def run: F[Unit] = SessionResource[F].useK {
    checkColumnExists("projects_tokens", "project_path") >>= {
      case true  => Kleisli.liftF(Logger[F].info("'project_path' column existed"))
      case false => addColumn()
    }
  }

  private def addColumn(): Kleisli[F, Session[F], Unit] = Kleisli { implicit session =>
    {
      for {
        _ <- execute(sql"ALTER TABLE projects_tokens ADD COLUMN IF NOT EXISTS project_path VARCHAR".command)
        _ <- execute(sql"ALTER TABLE projects_tokens ALTER COLUMN project_path SET NOT NULL".command)
        _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_path ON projects_tokens(project_path)".command)
        _ <- Logger[F].info("'project_path' column added")
      } yield ()
    } recoverWith logging
  }

  private lazy val logging: PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    Logger[F].error(exception)("'project_path' column adding failure")
    exception.raiseError[F, Unit]
  }
}
