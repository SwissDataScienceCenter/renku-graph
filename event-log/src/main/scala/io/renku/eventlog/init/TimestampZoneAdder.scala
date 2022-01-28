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
import cats.data.Kleisli._
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.eventlog.EventLogDB
import org.typelevel.log4cats.Logger
import skunk._
import skunk.codec.all._
import skunk.implicits._

import scala.util.control.NonFatal

private trait TimestampZoneAdder[F[_]] {
  def run(): F[Unit]
}

private object TimestampZoneAdder {
  def apply[F[_]: MonadCancelThrow: Logger](sessionResource: SessionResource[F, EventLogDB]): TimestampZoneAdder[F] =
    TimestampZoneAdderImpl(sessionResource)
}

private case class TimestampZoneAdderImpl[F[_]: MonadCancelThrow: Logger](
    sessionResource: SessionResource[F, EventLogDB]
) extends TimestampZoneAdder[F]
    with EventTableCheck {

  override def run(): F[Unit] = sessionResource.useK {
    columnsToMigrate.map { case (table, column) => migrateIfNeeded(table, column) }.sequence.void
  }

  private val columnsToMigrate = List(
    "event"                           -> "batch_date",
    "event"                           -> "created_date",
    "event"                           -> "execution_date",
    "event"                           -> "event_date",
    "subscription_category_sync_time" -> "last_synced",
    "project"                         -> "latest_event_date"
  )

  private def migrateIfNeeded(table: String, column: String): Kleisli[F, Session[F], Unit] = {
    findCurrentType(table, column) >>= {
      case "timestamp with time zone" =>
        liftF(Logger[F].info(s"$table.$column already migrated to 'timestamp with time zone'"))
      case columnType =>
        liftF[F, Session[F], Unit](Logger[F].info(s"$table.$column in '$columnType', migrating")) >>=
          (_ => migrate(table, column))
    }
  } recoverWith { case NonFatal(e) =>
    liftF(Logger[F].error(e)(s"$table.$column migration failed")) >> liftF(e.raiseError[F, Unit])
  }

  private def findCurrentType(table: String, column: String): Kleisli[F, Session[F], String] = {
    val query: Query[(String, String), String] =
      sql"""
           SELECT data_type
           FROM information_schema.columns
           WHERE table_name = $varchar AND column_name = $varchar"""
        .query(varchar)
    Kleisli {
      _.prepare(query)
        .use(_.unique(table -> column))
    }
  }

  private def migrate(table: String, column: String) = execute(
    sql"ALTER table #$table ALTER #$column TYPE timestamptz USING #$column AT TIME ZONE 'CEST'".command
  )
}
