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

private trait EventPayloadSchemaVersionAdder[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object EventPayloadSchemaVersionAdder {
  def apply[Interpretation[_]: Async: Bracket[*[_], Throwable]](
      transactor: SessionResource[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  ): EventPayloadSchemaVersionAdder[Interpretation] =
    new EventPayloadSchemaVersionAdderImpl(transactor, logger)
}

private class EventPayloadSchemaVersionAdderImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    transactor: SessionResource[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
) extends EventPayloadSchemaVersionAdder[Interpretation]
    with EventTableCheck {

  import cats.syntax.all._

  override def run(): Interpretation[Unit] = transactor.use { implicit session =>
    checkTableExists flatMap {
      case true => alterTable
      case false =>
        new Exception("Event payload table missing; alteration is not possible").raiseError[Interpretation, Unit]
    }
  }
  private def checkTableExists(implicit session: Session[Interpretation]): Interpretation[Boolean] = {
    val query: Query[Void, Boolean] =
      sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'event_payload')"
        .query(bool)

    session.unique(query).recover { case _ => false }
  }

  private def alterTable = transactor.use { implicit session =>
    for {
      _ <- session.execute(alterTableSql)
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_schema_version ON event_payload(schema_version)".command)
      _ <- logger info "'event_payload' table altered"
    } yield ()
  }

  private lazy val alterTableSql: Command[Void] = sql"""
    ALTER TABLE event_payload
    ALTER COLUMN payload SET NOT NULL,
    ADD COLUMN IF NOT EXISTS schema_version text NOT NULL,
    DROP CONSTRAINT IF EXISTS event_payload_pkey,
    ADD PRIMARY KEY (event_id, project_id, schema_version)
    """.command
}
