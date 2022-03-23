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
import io.renku.eventlog.EventLogDB.SessionResource
import org.typelevel.log4cats.Logger

private trait TSMigrationTableCreator[F[_]] extends DbMigrator[F]

private object TSMigrationTableCreator {
  def apply[F[_]: MonadCancelThrow: Logger: SessionResource]: TSMigrationTableCreator[F] =
    new TSMigrationTableCreatorImpl[F]
}

private class TSMigrationTableCreatorImpl[F[_]: MonadCancelThrow: Logger: SessionResource]
    extends TSMigrationTableCreator[F] {

  import cats.syntax.all._
  import skunk._
  import skunk.codec.all._
  import skunk.implicits._

  override def run(): F[Unit] = SessionResource[F].useK {
    checkTableExists >>= {
      case true  => Kleisli.liftF(Logger[F] info "'ts_migration' table exists")
      case false => createTable()
    }
  }

  private lazy val checkTableExists: Kleisli[F, Session[F], Boolean] = {
    val query: Query[Void, Boolean] =
      sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'ts_migration')".query(bool)
    Kleisli(_.unique(query) recover { case _ => false })
  }

  private def createTable(): Kleisli[F, Session[F], Unit] = for {
    _ <- execute(createTableSql)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_subscriber_version ON ts_migration(subscriber_version)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_subscriber_url ON ts_migration(subscriber_url)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_status ON ts_migration(status)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_change_date ON ts_migration(change_date)".command)
    _ <- Kleisli.liftF(Logger[F] info "'ts_migration' table created")
  } yield ()

  private lazy val createTableSql: Command[Void] = sql"""
    CREATE TABLE IF NOT EXISTS ts_migration(
      subscriber_version   VARCHAR                  NOT NULL,
      subscriber_url       VARCHAR                  NOT NULL,
      status               VARCHAR                  NOT NULL,
      change_date          TIMESTAMP WITH TIME ZONE NOT NULL,
      message              TEXT                     NOT NULL,
      PRIMARY KEY (subscriber_version, subscriber_url)
    );
  """.command
}
