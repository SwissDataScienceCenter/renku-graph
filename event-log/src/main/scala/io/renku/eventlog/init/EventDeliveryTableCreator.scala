/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import io.renku.eventlog.EventLogDB.SessionResource
import org.typelevel.log4cats.Logger
import skunk._
import skunk.implicits._

private trait EventDeliveryTableCreator[F[_]] extends DbMigrator[F]

private class EventDeliveryTableCreatorImpl[F[_]: MonadCancelThrow: Logger: SessionResource]
    extends EventDeliveryTableCreator[F] {

  import MigratorTools._
  import cats.syntax.all._

  override def run: F[Unit] = SessionResource[F].useK {
    checkTableExists("event_delivery") >>= {
      case true  => Kleisli.liftF(Logger[F] info "'event_delivery' table exists")
      case false => createTable()
    }
  }

  private def createTable(): Kleisli[F, Session[F], Unit] = for {
    _ <- execute(createTableSql)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_delivery_event_id    ON event_delivery(event_id)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_delivery_project_id  ON event_delivery(project_id)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_delivery_delivery_id ON event_delivery(delivery_id)".command)
    _ <- Kleisli.liftF(Logger[F] info "'event_delivery' table created")
    _ <- execute(foreignKeySql)
  } yield ()

  private lazy val createTableSql: Command[Void] = sql"""
    CREATE TABLE IF NOT EXISTS event_delivery(
      event_id     VARCHAR     NOT NULL,
      project_id   INT4        NOT NULL,
      delivery_id  VARCHAR(19) NOT NULL,
      PRIMARY KEY (event_id, project_id)
    )
    """.command

  private lazy val foreignKeySql: Command[Void] = sql"""
    ALTER TABLE event_delivery
    ADD CONSTRAINT fk_event FOREIGN KEY (event_id, project_id) REFERENCES event (event_id, project_id)
  """.command
}

private object EventDeliveryTableCreator {
  def apply[F[_]: MonadCancelThrow: Logger: SessionResource]: EventDeliveryTableCreator[F] =
    new EventDeliveryTableCreatorImpl[F]
}
