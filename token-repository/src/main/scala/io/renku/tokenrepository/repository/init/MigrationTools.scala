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

import cats.MonadThrow
import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import skunk.codec.all._
import skunk.implicits._
import skunk._

private object MigrationTools {

  def execute[F[_]: MonadThrow](sql: Command[skunk.Void])(implicit session: Session[F]): F[Unit] =
    session.execute(sql).void

  def checkColumnExists[F[_]: MonadCancelThrow](table: String, column: String): Kleisli[F, Session[F], Boolean] =
    Kleisli { session =>
      val query: Query[String ~ String, Boolean] =
        sql"""SELECT EXISTS (
            SELECT *
            FROM information_schema.columns
            WHERE table_name = $varchar AND column_name = $varchar
          )""".query(bool)
      session
        .prepare(query)
        .use(_.unique(table ~ column))
        .recover { case _ => false }
    }
}
