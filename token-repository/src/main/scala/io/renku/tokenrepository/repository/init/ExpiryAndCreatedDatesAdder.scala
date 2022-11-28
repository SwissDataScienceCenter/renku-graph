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

package io.renku.tokenrepository.repository
package init

import ProjectsTokensDB.SessionResource
import cats.data.Kleisli
import cats.effect.Spawn
import cats.syntax.all._
import org.typelevel.log4cats.Logger

private object ExpiryAndCreatedDatesAdder {
  def apply[F[_]: Spawn: Logger: SessionResource]: DBMigration[F] = new ExpiryAndCreatedDatesAdder[F]
}

private class ExpiryAndCreatedDatesAdder[F[_]: Spawn: Logger: SessionResource] extends DBMigration[F] {

  import MigrationTools._
  import skunk._
  import skunk.implicits._

  override def run(): F[Unit] = SessionResource[F].useK {
    addIfNotExists(column = "expiry_date", "DATE") >> addIfNotExists(column = "created_at", "TIMESTAMPTZ")
  }

  private def addIfNotExists(column: String, columnType: String) =
    checkColumnExists("projects_tokens", column) >>= {
      case true  => Kleisli.liftF(Logger[F].info(s"'$column' column existed"))
      case false => addColumn(column, columnType)
    }

  private def addColumn(column: String, columnType: String): Kleisli[F, Session[F], Unit] = Kleisli {
    implicit session =>
      for {
        _ <- execute(sql"ALTER TABLE projects_tokens ADD COLUMN IF NOT EXISTS #$column #$columnType".command)
        _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_#$column ON projects_tokens(#$column)".command)
        _ <- Logger[F].info(s"'$column' column added")
      } yield ()
  }
}
