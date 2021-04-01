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
import skunk._
import skunk.codec.all.bool
import skunk.implicits._

private trait EventDeliveryTableCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private class EventDeliveryTableCreatorImpl[Interpretation[_]](
    transactor: SessionResource[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends EventDeliveryTableCreator[Interpretation] {

  import cats.syntax.all._

  override def run(): Interpretation[Unit] =
    checkTableExists flatMap {
      case true => logger info "'event_delivery' table exists"
      case false =>
        transactor.use { implicit session =>
          session.transaction.use { xa =>
            for {
              sp <- xa.savepoint
              _ <- createTable recoverWith { e =>
                     xa.rollback(sp).flatMap(_ => e.raiseError[Interpretation, Unit])
                   }
            } yield ()
          }
        }
    }

  private def checkTableExists: Interpretation[Boolean] = transactor.use { session =>
    val query: Query[skunk.Void, Boolean] =
      sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'event_delivery')"
        .query(bool)
    session
      .unique(query)
      .recover { case _ => false }
  }

  private def createTable(implicit session: Session[Interpretation]) = for {
    _ <- session.execute(createTableSql)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_id    ON event_delivery(event_id)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_id  ON event_delivery(project_id)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_delivery_id ON event_delivery(delivery_id)".command)
    _ <- logger info "'event_delivery' table created"
    _ <- session.execute(foreignKeySql)
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
  def apply[Interpretation[_]](
      transactor: SessionResource[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): EventDeliveryTableCreator[Interpretation] =
    new EventDeliveryTableCreatorImpl(transactor, logger)
}
