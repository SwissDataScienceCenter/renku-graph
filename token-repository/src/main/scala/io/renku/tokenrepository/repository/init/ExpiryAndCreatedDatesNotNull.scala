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

package io.renku.tokenrepository.repository.init

import cats.data.Kleisli
import cats.effect.Spawn
import cats.syntax.all._
import io.renku.tokenrepository.repository.ProjectsTokensDB.SessionResource
import org.typelevel.log4cats.Logger

private object ExpiryAndCreatedDatesNotNull {
  def apply[F[_]: Spawn: Logger: SessionResource]: DBMigration[F] = new ExpiryAndCreatedDatesNotNull[F]
}

private class ExpiryAndCreatedDatesNotNull[F[_]: Spawn: Logger: SessionResource] extends DBMigration[F] {

  import MigrationTools._

  override def run(): F[Unit] = SessionResource[F].useK {
    ensureNotNullable("expiry_date") >> ensureNotNullable("created_at")
  }

  private def ensureNotNullable(column: String) =
    checkColumnNullable("projects_tokens", column) >>= {
      case true =>
        madeColumnNullable("projects_tokens", column)
          .flatMapF(_ => Logger[F].info(s"'$column' column made NOT NULL"))
      case false =>
        Kleisli.liftF(Logger[F].info(s"'$column' column already NOT NULL"))
    }
}
