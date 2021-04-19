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
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus.GenerationRecoverableFailure
import org.typelevel.log4cats.Logger
import io.renku.eventlog.{EventLogDB, TypeSerializers}
import skunk._
import skunk.codec.all._
import skunk.implicits._

private trait EventLogTableCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object EventLogTableCreator {
  def apply[Interpretation[_]: Async: Bracket[*[_], Throwable]](
      transactor: SessionResource[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  ): EventLogTableCreator[Interpretation] =
    new EventLogTableCreatorImpl(transactor, logger)
}

private class EventLogTableCreatorImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    transactor: SessionResource[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
) extends EventLogTableCreator[Interpretation]
    with EventTableCheck
    with TypeSerializers {

  import cats.syntax.all._

  override def run(): Interpretation[Unit] = transactor.use { implicit session =>
    whenEventTableExists(
      logger info "'event_log' table creation skipped",
      otherwise = checkTableExists flatMap {
        case true  => logger info "'event_log' table exists"
        case false => createTable
      }
    )
  }

  private def checkTableExists: Interpretation[Boolean] =
    transactor.use { session =>
      val query: Query[Void, Boolean] = sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'event_log')"
        .query(bool)
      session
        .unique(query)
        .recover { case _ => false }
    }

  private def createTable(implicit session: Session[Interpretation]) =
    for {
      _ <- execute(createTableSql)
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_id ON event_log(project_id)".command)
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_id ON event_log(event_id)".command)
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_status ON event_log(status)".command)
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_execution_date ON event_log(execution_date DESC)".command)
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_date ON event_log(event_date DESC)".command)
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_created_date ON event_log(created_date DESC)".command)
      _ <- revertStatusToGenerationRecoverableFailure
      _ <- logger info "'event_log' table created"
    } yield ()

  private lazy val createTableSql: Command[Void] = sql"""
    CREATE TABLE IF NOT EXISTS event_log(
      event_id       varchar   NOT NULL,
      project_id     int4      NOT NULL,
      status         varchar   NOT NULL,
      created_date   timestamp NOT NULL,
      execution_date timestamp NOT NULL,
      event_date     timestamp NOT NULL,
      event_body     text      NOT NULL,
      message        varchar,
      PRIMARY KEY (event_id, project_id)
    );
    """.command

  private lazy val revertStatusToGenerationRecoverableFailure = transactor.use { session =>
    val query: Command[EventStatus] =
      sql"UPDATE event_log set status=$eventStatusPut where status='TRIPLES_STORE_FAILURE'".command
    session.prepare(query).use(_.execute(GenerationRecoverableFailure))
  }

}
