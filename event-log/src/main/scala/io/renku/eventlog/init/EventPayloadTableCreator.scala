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

import cats.MonadError
import cats.effect.{Async, Bracket}
import ch.datascience.db.SessionResource
import org.typelevel.log4cats.Logger
import io.renku.eventlog.EventLogDB
import skunk.codec.all._
import skunk._
import skunk.implicits._

private trait EventPayloadTableCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object EventPayloadTableCreator {
  def apply[Interpretation[_]: Async: Bracket[*[_], Throwable]](
      transactor: SessionResource[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  ): EventPayloadTableCreator[Interpretation] =
    new EventPayloadTableCreatorImpl(transactor, logger)
}

private class EventPayloadTableCreatorImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    transactor: SessionResource[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
) extends EventPayloadTableCreator[Interpretation]
    with EventTableCheck {

  import cats.syntax.all._

  override def run(): Interpretation[Unit] = transactor.use { implicit session =>
    whenEventTableExists(
      checkTableExists flatMap {
        case true => logger info "'event_payload' table exists"
        case false =>
          session.transaction.use { xa =>
            for {
              sp <- xa.savepoint
              result <- createTable recoverWith { e =>
                          xa.rollback(sp).flatMap(_ => e.raiseError[Interpretation, Unit])
                        }
            } yield result
          }
      },
      otherwise = MonadError[Interpretation, Throwable].raiseError(
        new Exception("Event table missing; creation of event_payload is not possible")
      )
    )
  }

  private def checkTableExists: Interpretation[Boolean] =
    transactor.use { session =>
      val query: Query[Void, Boolean] =
        sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'event_payload')"
          .query(bool)
      session.unique(query).recover { case _ => false }
    }

  private def createTable(implicit session: Session[Interpretation]) = for {
    _ <- session.execute(createTableSql)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_id ON event_payload(event_id)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_id ON event_payload(project_id)".command)
    _ <- logger info "'event_payload' table created"
    _ <- session.execute(foreignKeySql)
  } yield ()

  private lazy val createTableSql: Command[Void] = sql"""
    CREATE TABLE IF NOT EXISTS event_payload(
      event_id       varchar   NOT NULL,
      project_id     int4      NOT NULL,
      payload        text,
      PRIMARY KEY (event_id, project_id)
    );
    """.command

  private lazy val foreignKeySql: Command[Void] = sql"""
    ALTER TABLE event_payload
    ADD CONSTRAINT fk_event FOREIGN KEY (event_id, project_id) REFERENCES event (event_id, project_id);
  """.command
}
