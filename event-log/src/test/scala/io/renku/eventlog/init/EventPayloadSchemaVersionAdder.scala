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
import io.renku.db.SessionResource
import io.renku.eventlog.EventLogDB
import org.typelevel.log4cats.Logger
import skunk._
import skunk.codec.all._
import skunk.implicits._

private trait EventPayloadSchemaVersionAdder[F[_]] {
  def run(): F[Unit]
}

private object EventPayloadSchemaVersionAdder {
  def apply[F[_]: MonadCancelThrow: Logger](
      sessionResource: SessionResource[F, EventLogDB]
  ): EventPayloadSchemaVersionAdder[F] =
    new EventPayloadSchemaVersionAdderImpl(sessionResource)
}

private class EventPayloadSchemaVersionAdderImpl[F[_]: MonadCancelThrow: Logger](
    sessionResource: SessionResource[F, EventLogDB]
) extends EventPayloadSchemaVersionAdder[F]
    with EventTableCheck {

  import cats.syntax.all._

  override def run(): F[Unit] = sessionResource.useK {
    checkTableExists >>= {
      case true => alterTable()
      case false =>
        Kleisli.liftF(
          new Exception("Event payload table missing; alteration is not possible").raiseError[F, Unit]
        )
    }
  }

  private lazy val checkTableExists: Kleisli[F, Session[F], Boolean] = {
    val query: Query[Void, Boolean] =
      sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'event_payload')"
        .query(bool)

    Kleisli(_.unique(query).recover { case _ => false })
  }

  private def alterTable(): Kleisli[F, Session[F], Unit] = for {
    _ <- execute(alterTableSql)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_schema_version ON event_payload(schema_version)".command)
    _ <- Kleisli.liftF(Logger[F] info "'event_payload' table altered")
  } yield ()

  private lazy val alterTableSql: Command[Void] =
    sql"""
      ALTER TABLE event_payload
      ALTER COLUMN payload SET NOT NULL,
      ADD COLUMN IF NOT EXISTS schema_version text NOT NULL,
      DROP CONSTRAINT IF EXISTS event_payload_pkey,
      ADD PRIMARY KEY (event_id, project_id, schema_version)
    """.command
}
