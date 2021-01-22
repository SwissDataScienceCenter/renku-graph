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

import cats.effect.Bracket
import ch.datascience.db.DbTransactor
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

private trait StatusesProcessingTimeTableCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object StatusesProcessingTimeTableCreator {
  def apply[Interpretation[_]](
      transactor: DbTransactor[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): StatusesProcessingTimeTableCreator[Interpretation] =
    new StatusesProcessingTimeTableCreatorImpl[Interpretation](transactor, logger)
}

private class StatusesProcessingTimeTableCreatorImpl[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends StatusesProcessingTimeTableCreator[Interpretation] {

  import cats.syntax.all._
  import doobie.implicits._
  private implicit val transact: DbTransactor[Interpretation, EventLogDB] = transactor

  override def run(): Interpretation[Unit] =
    checkTableExists flatMap {
      case true  => logger info "'status_processing_time' table exists"
      case false => createTable
    }

  private def checkTableExists: Interpretation[Boolean] =
    sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'status_processing_time')"
      .query[Boolean]
      .unique
      .transact(transactor.get)
      .recover { case _ => false }

  private def createTable = for {
    _ <- createTableSql.run transact transactor.get
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_id       ON status_processing_time(event_id)")
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_id     ON status_processing_time(project_id)")
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_status         ON status_processing_time(status)")
    _ <- logger info "'status_processing_time' table created"
    _ <- foreignKeySql.run transact transactor.get
  } yield ()

  private lazy val createTableSql = sql"""
    CREATE TABLE IF NOT EXISTS status_processing_time(
      event_id          varchar   NOT NULL,
      project_id        int4      NOT NULL,
      status            varchar   NOT NULL,
      processing_time   interval    NOT NULL,
      PRIMARY KEY (event_id, project_id, status)
    );
    """.update

  private lazy val foreignKeySql = sql"""
    ALTER TABLE status_processing_time
    ADD CONSTRAINT fk_event FOREIGN KEY (event_id, project_id) REFERENCES event (event_id, project_id);
  """.update
}
