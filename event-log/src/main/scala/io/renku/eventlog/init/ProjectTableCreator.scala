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

private trait ProjectTableCreator[F[_]] extends DbMigrator[F]

private object ProjectTableCreator {
  def apply[F[_]: MonadCancelThrow: Logger: SessionResource]: ProjectTableCreator[F] = new ProjectTableCreatorImpl[F]
}

private class ProjectTableCreatorImpl[F[_]: MonadCancelThrow: Logger: SessionResource]
    extends ProjectTableCreator[F]
    with EventTableCheck {

  import cats.syntax.all._

  override def run(): F[Unit] = SessionResource[F].useK {
    whenEventTableExists(
      Kleisli.liftF(Logger[F] info "'project' table creation skipped"),
      otherwise = checkTableExists >>= {
        case true  => Kleisli.liftF(Logger[F] info "'project' table exists")
        case false => createTable()
      }
    )
  }

  private lazy val checkTableExists: Kleisli[F, Session[F], Boolean] = {
    val query: Query[Void, Boolean] =
      sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'project')".query(bool)
    Kleisli(_.unique(query).recover { case _ => false })
  }

  private def createTable(): Kleisli[F, Session[F], Unit] =
    for {
      _ <- execute(createTableSql)
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_id        ON project(project_id)".command)
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_path      ON project(project_path)".command)
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_latest_event_date ON project(latest_event_date)".command)
      _ <- Kleisli.liftF(Logger[F] info "'project' table created")
      _ <- execute(fillInTableSql)
      _ <- Kleisli.liftF(Logger[F] info "'project' table filled in")
      _ <- execute(foreignKeySql)
    } yield ()

  private lazy val createTableSql: Command[Void] = sql"""
    CREATE TABLE IF NOT EXISTS project(
      project_id        int4      NOT NULL,
      project_path      VARCHAR   NOT NULL,
      latest_event_date timestamp NOT NULL,
      PRIMARY KEY (project_id)
    );
    """.command

  private lazy val fillInTableSql: Command[Void] = sql"""
    INSERT INTO project
    SELECT DISTINCT
      log.project_id,
      log.project_path,
      project_event_date.latest_event_date
    FROM (
      SELECT
        project_id,
        MAX(event_date) latest_event_date
      FROM event_log
      GROUP BY project_id
    ) project_event_date
    JOIN event_log log ON log.project_id = project_event_date.project_id AND log.event_date = project_event_date.latest_event_date
    """.command

  private lazy val foreignKeySql: Command[Void] = sql"""
    ALTER TABLE event_log
    ADD CONSTRAINT fk_project
    FOREIGN KEY (project_id) 
    REFERENCES project (project_id)
  """.command
}
