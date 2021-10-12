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

import cats.data.Kleisli
import cats.effect.BracketThrow
import cats.syntax.all._
import ch.datascience.db.SessionResource
import io.renku.eventlog.EventLogDB
import org.typelevel.log4cats.Logger
import skunk.codec.all.varchar
import skunk.implicits.toStringOps
import skunk.{Query, Session, Void}

private trait PayloadTypeChanger[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object PayloadTypeChanger {
  def apply[Interpretation[_]: BracketThrow: Logger](
      sessionResource: SessionResource[Interpretation, EventLogDB]
  ): PayloadTypeChanger[Interpretation] = new PayloadTypeChangerImpl(sessionResource)
}

private class PayloadTypeChangerImpl[Interpretation[_]: BracketThrow: Logger](
    sessionResource: SessionResource[Interpretation, EventLogDB]
) extends PayloadTypeChanger[Interpretation] {

  override def run(): Interpretation[Unit] = sessionResource.useK {
    checkIfAlreadyMigrated >>= {
      case true =>
        Kleisli.liftF(Logger[Interpretation].info("event_payload.payload already in bytea type"))
      case false => migrate
    }
  }

  private lazy val checkIfAlreadyMigrated: Kleisli[Interpretation, Session[Interpretation], Boolean] = {
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
    _ <- execute(sql"ALTER TABLE event_payload ALTER payload TYPE bytea USING payload::bytea".command)
    _ <- execute(sql"ALTER TABLE event_payload DROP COLUMN IF EXISTS schema_version".command)
  } yield ()

}
