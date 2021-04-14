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
import org.typelevel.log4cats.Logger
import io.renku.eventlog.EventLogDB

private trait SubscriberTableCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private class SubscriberTableCreatorImpl[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends SubscriberTableCreator[Interpretation] {

  import cats.syntax.all._
  import doobie.implicits._
  private implicit val transact: DbTransactor[Interpretation, EventLogDB] = transactor

  override def run(): Interpretation[Unit] =
    checkTableExists flatMap {
      case true  => logger info "'subscriber' table exists"
      case false => createTable
    }

  private def checkTableExists: Interpretation[Boolean] =
    sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'subscriber')"
      .query[Boolean]
      .unique
      .transact(transactor.get)
      .recover { case _ => false }

  private def createTable = for {
    _ <- createTableSql.run transact transactor.get
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_delivery_id ON subscriber(delivery_id)")
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_delivery_url ON subscriber(delivery_url)")
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_source_url ON subscriber(source_url)")
    _ <- logger info "'subscriber' table created"
  } yield ()

  private lazy val createTableSql = sql"""
    CREATE TABLE IF NOT EXISTS subscriber(
      delivery_id  VARCHAR(19) NOT NULL,
      delivery_url VARCHAR     NOT NULL,
      source_url   VARCHAR     NOT NULL,
      PRIMARY KEY (delivery_url, source_url)
    )
    """.update

}

private object SubscriberTableCreator {
  def apply[Interpretation[_]](
      transactor: DbTransactor[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): SubscriberTableCreator[Interpretation] =
    new SubscriberTableCreatorImpl(transactor, logger)
}
