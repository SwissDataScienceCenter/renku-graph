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

private trait StatusChangeEventsTableCreator[F[_]] extends DbMigrator[F]

private object StatusChangeEventsTableCreator {
  def apply[F[_]: MonadCancelThrow: Logger](
      sessionResource: SessionResource[F, EventLogDB]
  ): StatusChangeEventsTableCreator[F] = new StatusChangeEventsTableCreatorImpl(sessionResource)
}

private class StatusChangeEventsTableCreatorImpl[F[_]: MonadCancelThrow: Logger](
    sessionResource: SessionResource[F, EventLogDB]
) extends StatusChangeEventsTableCreator[F] {

  import cats.syntax.all._
  import skunk._
  import skunk.codec.all._
  import skunk.implicits._

  override def run(): F[Unit] = sessionResource.useK {
    checkTableExists flatMap {
      case true  => Kleisli.liftF(Logger[F] info "'status_change_events_queue' table exists")
      case false => createTable()
    }
  }

  private def checkTableExists: Kleisli[F, Session[F], Boolean] = {
    val query: Query[Void, Boolean] =
      sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'status_change_events_queue')".query(bool)
    Kleisli(_.unique(query).recover { case _ => false })
  }

  private def createTable(): Kleisli[F, Session[F], Unit] = for {
    _ <- execute(createTableSql)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_date ON status_change_events_queue(date)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_type ON status_change_events_queue(event_type)".command)
    _ <- Kleisli.liftF(Logger[F] info "'status_change_events_queue' table created")
  } yield ()

  private lazy val createTableSql: Command[Void] = sql"""
    CREATE TABLE IF NOT EXISTS status_change_events_queue(
      id         SERIAL    PRIMARY KEY,
      date       TIMESTAMP NOT NULL,
      event_type VARCHAR   NOT NULL,
      payload    TEXT      NOT NULL
    );
    """.command
}
