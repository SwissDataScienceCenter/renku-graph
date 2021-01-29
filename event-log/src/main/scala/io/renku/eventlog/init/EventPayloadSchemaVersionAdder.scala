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

private trait EventPayloadSchemaVersionAdder[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object EventPayloadSchemaVersionAdder {
  def apply[Interpretation[_]](
      transactor: DbTransactor[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): EventPayloadSchemaVersionAdder[Interpretation] =
    new EventPayloadSchemaVersionAdderImpl(transactor, logger)
}

private class EventPayloadSchemaVersionAdderImpl[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends EventPayloadSchemaVersionAdder[Interpretation]
    with EventTableCheck[Interpretation] {

  import cats.syntax.all._
  import doobie.implicits._

  private implicit val transact: DbTransactor[Interpretation, EventLogDB] = transactor

  override def run(): Interpretation[Unit] =
    checkTableExists flatMap {
      case true  => alterTable
      case false => ME.raiseError(new Exception("Event payload table missing; alteration is not possible"))
    }

  private def checkTableExists: Interpretation[Boolean] =
    sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'event_payload')"
      .query[Boolean]
      .unique
      .transact(transactor.get)
      .recover { case _ => false }

  private def alterTable = for {
    _ <- alterTableSql.update.run transact transactor.get
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_schema_version ON event_payload(schema_version)")
    _ <- logger info "'event_payload' table altered"
  } yield ()

  private lazy val alterTableSql = sql"""
    ALTER TABLE event_payload
    ALTER COLUMN payload SET NOT NULL,
    ADD COLUMN IF NOT EXISTS schema_version text NOT NULL,
    DROP CONSTRAINT IF EXISTS event_payload_pkey,
    ADD PRIMARY KEY (event_id, project_id, schema_version)
    """
}
