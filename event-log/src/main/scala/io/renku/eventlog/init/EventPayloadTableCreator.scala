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
import ch.datascience.db.SessionResource
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

private trait EventPayloadTableCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object EventPayloadTableCreator {
  def apply[Interpretation[_]](
      transactor: SessionResource[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): EventPayloadTableCreator[Interpretation] =
    new EventPayloadTableCreatorImpl(transactor, logger)
}

private class EventPayloadTableCreatorImpl[Interpretation[_]](
    transactor: SessionResource[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends EventPayloadTableCreator[Interpretation]
    with EventTableCheck[Interpretation] {

  import cats.syntax.all._
  import doobie.implicits._

  private implicit val transact: SessionResource[Interpretation, EventLogDB] = transactor

  override def run(): Interpretation[Unit] =
    whenEventTableExists(
      checkTableExists flatMap {
        case true  => logger info "'event_payload' table exists"
        case false => createTable
      },
      otherwise = ME.raiseError(new Exception("Event table missing; creation of event_payload is not possible"))
    )

  private def checkTableExists: Interpretation[Boolean] =
    sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'event_payload')"
      .query[Boolean]
      .unique
      .transact(transactor.resource)
      .recover { case _ => false }

  private def createTable = for {
    _ <- createTableSql.update.run transact transactor.resource
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_id ON event_payload(event_id)")
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_id ON event_payload(project_id)")
    _ <- logger info "'event_payload' table created"
    _ <- foreignKeySql.run transact transactor.resource
  } yield ()

  private lazy val createTableSql = sql"""
    CREATE TABLE IF NOT EXISTS event_payload(
      event_id       varchar   NOT NULL,
      project_id     int4      NOT NULL,
      payload        text,
      PRIMARY KEY (event_id, project_id)
    );
    """

  private lazy val foreignKeySql = sql"""
    ALTER TABLE event_payload
    ADD CONSTRAINT fk_event FOREIGN KEY (event_id, project_id) REFERENCES event (event_id, project_id);
  """.update
}
