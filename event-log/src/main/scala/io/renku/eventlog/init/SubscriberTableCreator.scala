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

import cats.effect.{Async, Bracket}
import ch.datascience.db.SessionResource
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB
import skunk._
import skunk.implicits._
import skunk.codec.all._

private trait SubscriberTableCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private class SubscriberTableCreatorImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    transactor: SessionResource[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
) extends SubscriberTableCreator[Interpretation] {

  import cats.syntax.all._

  override def run(): Interpretation[Unit] =
    checkTableExists flatMap {
      case true => logger info "'subscriber' table exists"
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

  private def checkTableExists: Interpretation[Boolean] =
    transactor.use { session =>
      val query: Query[Void, Boolean] = sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'subscriber')"
        .query(bool)
      session.unique(query).recover { case _ => false }
    }

  private def createTable(implicit session: Session[Interpretation]) = for {
    _ <- execute(createTableSql)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_delivery_id ON subscriber(delivery_id)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_delivery_url ON subscriber(delivery_url)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_source_url ON subscriber(source_url)".command)
    _ <- logger info "'subscriber' table created"
  } yield ()

  private lazy val createTableSql: Command[Void] = sql"""
    CREATE TABLE IF NOT EXISTS subscriber(
      delivery_id  VARCHAR(19) NOT NULL,
      delivery_url VARCHAR     NOT NULL,
      source_url   VARCHAR     NOT NULL,
      PRIMARY KEY (delivery_url, source_url)
    )
    """.command

}

private object SubscriberTableCreator {
  def apply[Interpretation[_]: Async: Bracket[*[_], Throwable]](
      transactor: SessionResource[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  ): SubscriberTableCreator[Interpretation] =
    new SubscriberTableCreatorImpl(transactor, logger)
}
