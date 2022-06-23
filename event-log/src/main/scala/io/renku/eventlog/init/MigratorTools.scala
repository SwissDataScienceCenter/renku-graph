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

package io.renku.eventlog.init

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import skunk.codec.all.bool
import skunk.implicits._
import skunk.{Command, Query, Session, Void}

private object MigratorTools {

  def whenTableExists[F[_]: MonadCancelThrow](table: String)(
      eventTableExistsMessage: => Kleisli[F, Session[F], Unit],
      otherwise:               => Kleisli[F, Session[F], Unit]
  ): Kleisli[F, Session[F], Unit] = checkTableExists(table) >>= {
    case true  => eventTableExistsMessage
    case false => otherwise
  }

  def checkTableExists[F[_]: MonadCancelThrow](table: String): Kleisli[F, Session[F], Boolean] = {
    val query: Query[Void, Boolean] = sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = '#$table')"
      .query(bool)
    Kleisli(_.unique(query).recover { case _ => false })
  }

  def checkColumnExists[F[_]: MonadCancelThrow](table: String, column: String): Kleisli[F, Session[F], Boolean] = {
    val query: Query[Void, Boolean] = sql"""
      SELECT EXISTS (
        SELECT *
        FROM information_schema.columns
        WHERE table_name = '#$table' AND column_name = '#$column'
      )""".query(bool)
    Kleisli(_.unique(query).recover { case _ => false })
  }

  def execute[F[_]: MonadCancelThrow](sql: Command[Void]): Kleisli[F, Session[F], Unit] =
    Kleisli(_.execute(sql).void)
}
