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
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.eventlog.EventLogDB
import org.typelevel.log4cats.Logger
import skunk.codec.all.varchar
import skunk.implicits.toStringOps
import skunk.{Query, Session, Void}

private trait PayloadTypeChanger[F[_]] {
  def run(): F[Unit]
}

private object PayloadTypeChanger {
  def apply[F[_]: MonadCancelThrow: Logger](
      sessionResource: SessionResource[F, EventLogDB]
  ): PayloadTypeChanger[F] = new PayloadTypeChangerImpl(sessionResource)
}

private class PayloadTypeChangerImpl[F[_]: MonadCancelThrow: Logger](
    sessionResource: SessionResource[F, EventLogDB]
) extends PayloadTypeChanger[F] {

  override def run(): F[Unit] = sessionResource.useK {
    checkIfAlreadyMigrated >>= {
      case true =>
        Kleisli.liftF(Logger[F].info("event_payload.payload already in bytea type"))
      case false => migrate
    }
  }

  private lazy val checkIfAlreadyMigrated: Kleisli[F, Session[F], Boolean] = {
    val query: Query[Void, String] =
      sql"""SELECT data_type FROM information_schema.columns
            WHERE column_name = 'payload' AND table_name = 'event_payload' 
            """.query(varchar)

    Kleisli {
      _.unique(query)
        .map {
          case "bytea" => true
          case _       => false
        }
        .recover(_ => false)
    }
  }

  private lazy val migrate = for {
    _ <- execute(sql"TRUNCATE TABLE event_payload".command)
    _ <- execute(sql"ALTER TABLE event_payload DROP CONSTRAINT IF EXISTS event_payload_pkey".command)
    _ <- execute(sql"ALTER TABLE event_payload ADD PRIMARY KEY (event_id, project_id)".command)
    _ <- execute(sql"ALTER TABLE event_payload ALTER payload TYPE bytea USING payload::bytea".command)
    _ <- execute(sql"DROP INDEX IF EXISTS idx_schema_version".command)
    _ <- execute(sql"ALTER TABLE event_payload DROP COLUMN IF EXISTS schema_version".command)
  } yield ()
}
