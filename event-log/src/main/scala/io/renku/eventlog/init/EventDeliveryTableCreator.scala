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
import ch.datascience.db.DbTransactor
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

private trait EventDeliveryTableCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private class EventDeliveryTableCreatorImpl[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends EventDeliveryTableCreator[Interpretation] {

  import cats.syntax.all._
  import doobie.implicits._
  private implicit val transact: DbTransactor[Interpretation, EventLogDB] = transactor

  override def run(): Interpretation[Unit] =
    checkTableExists flatMap {
      case true  => logger info "'event_delivery' table exists"
      case false => createTable
    }

  private def checkTableExists: Interpretation[Boolean] =
    sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'event_delivery')"
      .query[Boolean]
      .unique
      .transact(transactor.get)
      .recover { case _ => false }

  private def createTable = for {
    _ <- createTableSql.run transact transactor.get
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_id    ON event_delivery(event_id)")
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_id  ON event_delivery(project_id)")
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_delivery_id ON event_delivery(delivery_id)")
    _ <- logger info "'event_delivery' table created"
    _ <- foreignKeySql.run transact transactor.get
  } yield ()

  private lazy val createTableSql = sql"""
    CREATE TABLE IF NOT EXISTS event_delivery(
      event_id     VARCHAR     NOT NULL,
      project_id   INT4        NOT NULL,
      delivery_id  VARCHAR(19) NOT NULL,
      PRIMARY KEY (event_id, project_id, delivery_id)
    )
    """.update

  private lazy val foreignKeySql = sql"""
    ALTER TABLE event_delivery
    ADD CONSTRAINT fk_event FOREIGN KEY (event_id, project_id) REFERENCES event (event_id, project_id)
  """.update
}

private object EventDeliveryTableCreator {
  def apply[Interpretation[_]](
      transactor: DbTransactor[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): EventDeliveryTableCreator[Interpretation] =
    new EventDeliveryTableCreatorImpl(transactor, logger)
}
