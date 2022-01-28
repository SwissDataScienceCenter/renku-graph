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
import skunk._
import skunk.codec.all._
import skunk.implicits._

private trait TimestampZoneAdder[F[_]] {
  def run(): F[Unit]
}

private object TimestampZoneAdder {
  def apply[F[_]: MonadCancelThrow: Logger](
      sessionResource: SessionResource[F, EventLogDB]
  ): TimestampZoneAdder[F] = TimestampZoneAdderImpl(sessionResource)
}

private case class TimestampZoneAdderImpl[F[_]: MonadCancelThrow: Logger](
    sessionResource: SessionResource[F, EventLogDB]
) extends TimestampZoneAdder[F]
    with EventTableCheck {
  override def run(): F[Unit] = sessionResource.useK {
    checkIfAlreadyTimestamptz >>= {
      case true =>
        Kleisli.liftF(Logger[F].info("Fields are already in timestamptz type"))
      case false => migrateTimestampToCEST()
    }
  }

  private val columnsToMigrate =
    List("batch_date", "created_date", "execution_date", "event_date", "last_synced", "latest_event_date")

  private lazy val checkIfAlreadyTimestamptz: Kleisli[F, Session[F], Boolean] = {
    val query: Query[Void, String ~ String] =
      sql"""
         SELECT column_name, data_type FROM information_schema.columns""".query(varchar ~ varchar)

    Kleisli {
      _.execute(query)
        .map {
          _.filter { case columnName ~ _ =>
            columnsToMigrate.contains(columnName)
          }.forall { case _ ~ columnType =>
            columnType == "timestamp with time zone"
          }
        }
        .recover(_ => false)
    }
  }

  private def migrateTimestampToCEST(): Kleisli[F, Session[F], Unit] =
    for {
      _ <-
        execute(
          sql"ALTER table event ALTER batch_date TYPE timestamptz USING batch_date AT TIME ZONE 'CEST' ".command
        )
      _ <-
        execute(
          sql"ALTER table event ALTER created_date TYPE timestamptz USING created_date AT TIME ZONE 'CEST' ".command
        )
      _ <-
        execute(
          sql"ALTER table event ALTER execution_date TYPE timestamptz USING execution_date AT TIME ZONE 'CEST' ".command
        )
      _ <-
        execute(
          sql"ALTER table event ALTER event_date TYPE timestamptz USING event_date AT TIME ZONE 'CEST' ".command
        )
      _ <-
        execute(
          sql"ALTER table subscription_category_sync_time ALTER last_synced TYPE timestamptz USING last_synced AT TIME ZONE 'CEST' ".command
        )
      _ <-
        execute(
          sql"ALTER table project ALTER latest_event_date TYPE timestamptz USING latest_event_date AT TIME ZONE 'CEST' ".command
        )
    } yield ()
}
