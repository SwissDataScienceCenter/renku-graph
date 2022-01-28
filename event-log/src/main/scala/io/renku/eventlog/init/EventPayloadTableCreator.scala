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

private trait EventPayloadTableCreator[F[_]] {
  def run(): F[Unit]
}

private object EventPayloadTableCreator {
  def apply[F[_]: MonadCancelThrow: Logger](
      sessionResource: SessionResource[F, EventLogDB]
  ): EventPayloadTableCreator[F] = new EventPayloadTableCreatorImpl(sessionResource)
}

private class EventPayloadTableCreatorImpl[F[_]: MonadCancelThrow: Logger](
    sessionResource: SessionResource[F, EventLogDB]
) extends EventPayloadTableCreator[F]
    with EventTableCheck {

  import cats.syntax.all._

  override def run(): F[Unit] = sessionResource.useK {
    whenEventTableExists(
      checkTableExists flatMap {
        case true  => Kleisli.liftF(Logger[F] info "'event_payload' table exists")
        case false => createTable()
      },
      otherwise = Kleisli.liftF(
        MonadCancelThrow[F].raiseError(
          new Exception("Event table missing; creation of event_payload is not possible")
        )
      )
    )
  }

  private def checkTableExists: Kleisli[F, Session[F], Boolean] = {
    val query: Query[Void, Boolean] =
      sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'event_payload')".query(bool)
    Kleisli(_.unique(query).recover { case _ => false })
  }

  private def createTable(): Kleisli[F, Session[F], Unit] = for {
    _ <- execute(createTableSql)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_id ON event_payload(event_id)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_id ON event_payload(project_id)".command)
    _ <- Kleisli.liftF(Logger[F] info "'event_payload' table created")
    _ <- execute(foreignKeySql)
  } yield ()

  private lazy val createTableSql: Command[Void] =
    sql"""
    CREATE TABLE IF NOT EXISTS event_payload(
      event_id       varchar   NOT NULL,
      project_id     int4      NOT NULL,
      payload        text,
      PRIMARY KEY (event_id, project_id)
    );
    """.command

  private lazy val foreignKeySql: Command[Void] =
    sql"""
    ALTER TABLE event_payload
    ADD CONSTRAINT fk_event FOREIGN KEY (event_id, project_id) REFERENCES event (event_id, project_id);
  """.command
}
