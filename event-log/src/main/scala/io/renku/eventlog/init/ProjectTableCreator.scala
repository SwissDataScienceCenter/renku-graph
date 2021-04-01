/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import cats.effect.{Async, Bracket}
import ch.datascience.db.SessionResource
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB
import skunk._
import skunk.implicits._
import skunk.codec.all._

private trait ProjectTableCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object ProjectTableCreator {
  def apply[Interpretation[_]: Async: Bracket[*[_], Throwable]](
      transactor: SessionResource[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  ): ProjectTableCreator[Interpretation] =
    new ProjectTableCreatorImpl(transactor, logger)
}

private class ProjectTableCreatorImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    transactor: SessionResource[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
) extends ProjectTableCreator[Interpretation]
    with EventTableCheck {

  import cats.syntax.all._

  override def run(): Interpretation[Unit] = transactor.use { implicit session =>
    whenEventTableExists(
      logger info "'project' table creation skipped",
      otherwise = checkTableExists flatMap {
        case true => logger info "'project' table exists"
        case false =>
          session.transaction.use { xa =>
            for {
              sp <- xa.savepoint
              _ <- createTable recoverWith { e =>
                     xa.rollback(sp).flatMap(_ => e.raiseError[Interpretation, Unit])
                   }
            } yield ()
          }
      }
    )
  }

  private def checkTableExists: Interpretation[Boolean] = transactor
    .use { session =>
      val query: Query[Void, Boolean] =
        sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'project')".query(bool)
      session.unique(query).recover { case _ => false }
    }
  private def createTable(implicit session: Session[Interpretation]) = for {
    _ <- execute(createTableSql)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_id        ON project(project_id)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_path      ON project(project_path)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_latest_event_date ON project(latest_event_date)".command)
    _ <- logger info "'project' table created"
    _ <- execute(fillInTableSql)
    _ <- logger info "'project' table filled in"
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
