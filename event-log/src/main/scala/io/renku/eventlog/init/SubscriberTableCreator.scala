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

import cats.data.Kleisli
import cats.effect.{Async, Bracket}
import ch.datascience.db.SessionResource
import org.typelevel.log4cats.Logger
import io.renku.eventlog.EventLogDB
import skunk._
import skunk.implicits._
import skunk.codec.all._

private trait SubscriberTableCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private class SubscriberTableCreatorImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    sessionResource: SessionResource[Interpretation, EventLogDB],
    logger:          Logger[Interpretation]
) extends SubscriberTableCreator[Interpretation] {

  import cats.syntax.all._

  override def run(): Interpretation[Unit] = sessionResource.useK {
    checkTableExists >>= {
      case true  => Kleisli.liftF(logger info "'subscriber' table exists")
      case false => createTable()

    }
  }

  private lazy val checkTableExists: Kleisli[Interpretation, Session[Interpretation], Boolean] = {
    val query: Query[Void, Boolean] = sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'subscriber')"
      .query(bool)
    Kleisli(_.unique(query).recover { case _ => false })
  }

  private def createTable(): Kleisli[Interpretation, Session[Interpretation], Unit] =
    for {
      _ <- execute(createTableSql)
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_delivery_id ON subscriber(delivery_id)".command)
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_delivery_url ON subscriber(delivery_url)".command)
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_source_url ON subscriber(source_url)".command)
      _ <- Kleisli.liftF(logger info "'subscriber' table created")
    } yield ()

  private lazy val createTableSql: Command[Void] =
    sql"""
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
      sessionResource: SessionResource[Interpretation, EventLogDB],
      logger:          Logger[Interpretation]
  ): SubscriberTableCreator[Interpretation] =
    new SubscriberTableCreatorImpl(sessionResource, logger)
}
