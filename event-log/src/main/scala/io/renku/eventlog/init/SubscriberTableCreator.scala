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
import io.renku.eventlog.EventLogDB.SessionResource
import org.typelevel.log4cats.Logger
import skunk._
import skunk.codec.all._
import skunk.implicits._

private trait SubscriberTableCreator[F[_]] extends DbMigrator[F]

private class SubscriberTableCreatorImpl[F[_]: MonadCancelThrow: Logger: SessionResource]
    extends SubscriberTableCreator[F] {

  import cats.syntax.all._

  override def run(): F[Unit] = SessionResource[F].useK {
    checkTableExists >>= {
      case true  => Kleisli.liftF(Logger[F] info "'subscriber' table exists")
      case false => createTable()

    }
  }

  private lazy val checkTableExists: Kleisli[F, Session[F], Boolean] = {
    val query: Query[Void, Boolean] = sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'subscriber')"
      .query(bool)
    Kleisli(_.unique(query).recover { case _ => false })
  }

  private def createTable(): Kleisli[F, Session[F], Unit] = for {
    _ <- execute(createTableSql)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_delivery_id ON subscriber(delivery_id)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_delivery_url ON subscriber(delivery_url)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_source_url ON subscriber(source_url)".command)
    _ <- Kleisli.liftF(Logger[F] info "'subscriber' table created")
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
  def apply[F[_]: MonadCancelThrow: Logger: SessionResource]: SubscriberTableCreator[F] =
    new SubscriberTableCreatorImpl[F]
}
