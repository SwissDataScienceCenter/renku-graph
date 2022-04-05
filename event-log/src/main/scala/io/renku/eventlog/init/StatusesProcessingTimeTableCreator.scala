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
import skunk._
import skunk.codec.all._
import skunk.implicits._

private trait StatusesProcessingTimeTableCreator[F[_]] extends DbMigrator[F]

private object StatusesProcessingTimeTableCreator {
  def apply[F[_]: MonadCancelThrow: Logger: SessionResource]: StatusesProcessingTimeTableCreator[F] =
    new StatusesProcessingTimeTableCreatorImpl[F]
}

private class StatusesProcessingTimeTableCreatorImpl[F[_]: MonadCancelThrow: Logger: SessionResource]
    extends StatusesProcessingTimeTableCreator[F] {

  import cats.syntax.all._

  override def run(): F[Unit] = SessionResource[F].useK {
    checkTableExists >>= {
      case true  => Kleisli.liftF(Logger[F] info "'status_processing_time' table exists")
      case false => createTable()
    }
  }

  private lazy val checkTableExists: Kleisli[F, Session[F], Boolean] = {
    val query: Query[Void, Boolean] =
      sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'status_processing_time')".query(bool)
    Kleisli[F, Session[F], Boolean] {
      _.unique(query)
        .recover { case _ => false }
    }
  }

  private def createTable() = for {
    _ <- execute(createTableSql)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_id       ON status_processing_time(event_id)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_id     ON status_processing_time(project_id)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_status         ON status_processing_time(status)".command)
    _ <- Kleisli.liftF(Logger[F] info "'status_processing_time' table created")
    _ <- execute(foreignKeySql)
  } yield ()

  private lazy val createTableSql: Command[Void] = sql"""
    CREATE TABLE IF NOT EXISTS status_processing_time(
      event_id          varchar   NOT NULL,
      project_id        int4      NOT NULL,
      status            varchar   NOT NULL,
      processing_time   interval    NOT NULL,
      PRIMARY KEY (event_id, project_id, status)
    );
    """.command

  private lazy val foreignKeySql: Command[Void] = sql"""
    ALTER TABLE status_processing_time
    ADD CONSTRAINT fk_event FOREIGN KEY (event_id, project_id) REFERENCES event (event_id, project_id);
  """.command
}
