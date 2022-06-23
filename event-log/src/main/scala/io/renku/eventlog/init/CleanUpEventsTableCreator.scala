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

private trait CleanUpEventsTableCreator[F[_]] extends DbMigrator[F]

private object CleanUpEventsTableCreator {
  def apply[F[_]: MonadCancelThrow: Logger: SessionResource]: CleanUpEventsTableCreator[F] =
    new CleanUpEventsTableCreatorImpl[F]
}

private class CleanUpEventsTableCreatorImpl[F[_]: MonadCancelThrow: Logger: SessionResource]
    extends CleanUpEventsTableCreator[F] {

  import MigratorTools._
  import cats.syntax.all._
  import skunk._
  import skunk.implicits._

  override def run(): F[Unit] = SessionResource[F].useK {
    checkTableExists("clean_up_events_queue") >>= {
      case true  => Kleisli.liftF(Logger[F] info "'clean_up_events_queue' table exists")
      case false => createTable()
    }
  }

  private def createTable(): Kleisli[F, Session[F], Unit] = for {
    _ <- execute(createTableSql)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_date ON clean_up_events_queue(date)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_path ON clean_up_events_queue(project_path)".command)
    _ <- execute(sql"ALTER TABLE clean_up_events_queue ADD CONSTRAINT project_path_unique UNIQUE(project_path)".command)
    _ <- Kleisli.liftF(Logger[F] info "'clean_up_events_queue' table created")
  } yield ()

  private lazy val createTableSql: Command[Void] = sql"""
    CREATE TABLE IF NOT EXISTS clean_up_events_queue(
      id           SERIAL                   PRIMARY KEY,
      date         TIMESTAMP WITH TIME ZONE NOT NULL,
      project_path VARCHAR                  NOT NULL
    );
    """.command
}
