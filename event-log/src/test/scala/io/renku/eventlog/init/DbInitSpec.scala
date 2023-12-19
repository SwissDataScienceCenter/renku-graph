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

package io.renku.eventlog
package init

import cats.effect.IO
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.db.SessionResource
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import org.scalatest.Suite
import skunk._
import skunk.codec.all.{bool, varchar}
import skunk.implicits._

trait DbInitSpec extends EventLogPostgresSpec {
  self: Suite =>

  protected val runMigrationsUpTo: Class[_ <: DbMigrator[IO]]

  override lazy val migrations: SessionResource[IO, EventLogDB] => IO[Unit] = { sr =>
    implicit val msr: EventLogDB.SessionResource[IO] = EventLogDB.SessionResource[IO](sr)
    DbInitializer
      .migrations[IO]
      .takeWhile(m => !runMigrationsUpTo.isAssignableFrom(m.getClass))
      .traverse_(_.run) >> logger.resetF()
  }

  def createEventTable(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    DbInitializer
      .migrations[IO]
      .takeWhile(_.getClass != classOf[EventLogTableRenamer[IO]])
      .traverse_(_.run)

  def dropTable(tableName: String)(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    moduleSessionResource.session.use { session =>
      val query: Command[Void] = sql"DROP TABLE IF EXISTS #$tableName CASCADE".command
      session.execute(query).void
    }

  def tableExists(tableName: String)(implicit cfg: DBConfig[EventLogDB]): IO[Boolean] =
    moduleSessionResource.session.use { session =>
      val query: Query[String, Boolean] =
        sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = $varchar)".query(bool)
      session.prepare(query).flatMap(_.unique(tableName)).recover { case _ => false }
    }

  def verifyColumnExists(table: String, column: String)(implicit cfg: DBConfig[EventLogDB]): IO[Boolean] =
    moduleSessionResource.session.use { session =>
      val query: Query[String *: String *: EmptyTuple, Boolean] =
        sql"""SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = $varchar AND column_name = $varchar
              )""".query(bool)
      session
        .prepare(query)
        .flatMap(_.unique(table *: column *: EmptyTuple))
        .recover { case _ => false }
    }

  def verifyConstraintExists(table: String, constraintName: String)(implicit cfg: DBConfig[EventLogDB]): IO[Boolean] =
    moduleSessionResource.session.use { session =>
      val query: Query[String *: String *: EmptyTuple, Boolean] =
        sql"""SELECT EXISTS (
                 SELECT *
                 FROM information_schema.constraint_column_usage
                 WHERE table_name = $varchar AND constraint_name = $varchar
               )""".query(bool)
      session
        .prepare(query)
        .flatMap(_.unique(table *: constraintName *: EmptyTuple))
        .recover { case _ => false }
    }

  def verifyIndexExists(table: String, indexName: String)(implicit cfg: DBConfig[EventLogDB]): IO[Boolean] =
    moduleSessionResource.session.use { session =>
      val query: Query[String *: String *: EmptyTuple, Boolean] =
        sql"""SELECT EXISTS (
                 SELECT *
                 FROM pg_indexes
                 WHERE tablename = $varchar AND indexname = $varchar
               )""".query(bool)
      session
        .prepare(query)
        .flatMap(_.unique(table *: indexName *: EmptyTuple))
        .recover { case _ => false }
    }

  def verifyExists(table: String, column: String, hasType: String)(implicit cfg: DBConfig[EventLogDB]): IO[Boolean] =
    moduleSessionResource.session.use { session =>
      val query: Query[String *: String *: EmptyTuple, String] =
        sql"""SELECT data_type
                FROM information_schema.columns
                WHERE table_name = $varchar AND column_name = $varchar;""".query(varchar)
      session
        .prepare(query)
        .flatMap(_.unique(table *: column *: EmptyTuple))
        .map(dataType => dataType == hasType)
        .recover { case _ => false }
    }

  def executeCommand(sql: Command[Void])(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    moduleSessionResource.session.use(_.execute(sql).void)
}
