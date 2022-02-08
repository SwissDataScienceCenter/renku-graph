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

private trait EventDeliveryEventTypeAdder[F[_]] {
  def run(): F[Unit]
}

private object EventDeliveryEventTypeAdder {
  def apply[F[_]: MonadCancelThrow: Logger](
      sessionResource: SessionResource[F, EventLogDB]
  ): EventDeliveryEventTypeAdder[F] = EventDeliveryEventTypeAdderImpl(sessionResource)
}

private case class EventDeliveryEventTypeAdderImpl[F[_]: MonadCancelThrow: Logger](
    sessionResource: SessionResource[F, EventLogDB]
) extends EventDeliveryEventTypeAdder[F] {
  override def run(): F[Unit] = sessionResource.useK {
    checkColumnExists() >>= {
      case true =>
        Kleisli.liftF(Logger[F] info "'event_type_id' column adding skipped")
      case false => addEventTypeCollumn()
    }
  }

  private def checkColumnExists(): Kleisli[F, Session[F], Boolean] = {
    val query: Query[Void, String] =
      sql"""SELECT column_name FROM information_schema.columns WHERE table_name = 'event_delivery'""".query(varchar)

    Kleisli {
      _.execute(query)
        .map(_.exists(_ == "event_type_id"))
        .recover(_ => false)
    }
  }

  private def addEventTypeCollumn(): Kleisli[F, Session[F], Unit] =
    for {
      _ <-
        execute(
          sql"ALTER TABLE event_delivery ADD COLUMN IF NOT EXISTS event_type_id varchar".command
        )
      _ <-
        execute(
          sql"ALTER TABLE event_delivery DROP CONSTRAINT IF EXISTS event_delivery_pkey".command
        )
      _ <-
        execute(
          sql"ALTER TABLE event_delivery ALTER COLUMN event_id DROP NOT NULL".command
        )
      _ <-
        execute(
          sql"ALTER TABLE event_delivery DROP CONSTRAINT IF EXISTS event_project_id_unique".command
        )
      _ <-
        execute(
          sql"ALTER TABLE event_delivery ADD CONSTRAINT event_project_id_unique UNIQUE (event_id, project_id)".command
        )
      _ <-
        execute(
          sql"ALTER TABLE event_delivery DROP CONSTRAINT IF EXISTS project_event_type_id_unique".command
        )
      _ <-
        execute(
          sql"ALTER TABLE event_delivery ADD CONSTRAINT project_event_type_id_unique UNIQUE (project_id, event_type_id)".command
        )
      _ <- Kleisli.liftF(Logger[F] info "'event_type_id' column added")
    } yield ()
}
