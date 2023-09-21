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

package io.renku.eventsqueue

import DBInfra.QueueTable
import cats.data.Kleisli
import cats.effect.kernel.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.eventsqueue.DBInfra.QueueTable.Column
import org.typelevel.log4cats.Logger
import skunk.codec.all.bool
import skunk.implicits._
import skunk.{Command, Query, Session, Void}

trait DBInfraCreator[F[_]] {
  def createDBInfra[DB](sr: SessionResource[F, DB]): F[Unit]
}

object DBInfraCreator {
  def apply[F[_]: MonadCancelThrow: Logger]: DBInfraCreator[F] = new DBInfraCreatorImpl[F]
}

private class DBInfraCreatorImpl[F[_]: MonadCancelThrow: Logger] extends DBInfraCreator[F] {

  override def createDBInfra[DB](sr: SessionResource[F, DB]): F[Unit] =
    sr.useK {
      checkTableExists() >>= {
        case true  => Kleisli.liftF(Logger[F].info(s"'${QueueTable.name}' already exists"))
        case false => createTable()
      }
    }

  private def createTable() = for {
    _ <- run(createEnqueueStatusTypeSql)
    _ <- run(createTableSql)
    _ <- run(createIndexSql("idx_enqueued_event_payload", Column.payload))
    _ <- run(createIndexSql("idx_enqueued_event_created", Column.created))
    _ <- run(createIndexSql("idx_enqueued_event_updated", Column.updated))
    _ <- run(createIndexSql("idx_enqueued_event_status", Column.status))
  } yield ()

  private def checkTableExists(): Kleisli[F, Session[F], Boolean] = {
    val query: Query[Void, Boolean] =
      sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = '#${QueueTable.name}')"
        .query(bool)
    Kleisli(_.unique(query).recover { case _ => false })
  }

  private lazy val createEnqueueStatusTypeSql: Command[Void] = sql"""
    CREATE TYPE ENQUEUE_STATUS AS ENUM (
      '#${EnqueueStatus.New.value}', '#${EnqueueStatus.Processing.value}'
    );
    """.command

  private lazy val createTableSql: Command[Void] = sql"""
    CREATE TABLE IF NOT EXISTS #${QueueTable.name}(
      id                            SERIAL                   PRIMARY KEY,
      #${Column.payload} TEXT                     NOT NULL,
      #${Column.created} TIMESTAMP WITH TIME ZONE NOT NULL,
      #${Column.updated} TIMESTAMP WITH TIME ZONE NOT NULL,
      #${Column.status}  ENQUEUE_STATUS           NOT NULL
    );
    """.command

  private def createIndexSql(index: String, column: String): Command[Void] =
    sql"CREATE INDEX IF NOT EXISTS #$index ON #${QueueTable.name}(#$column)".command

  private def run(sql: Command[Void]): Kleisli[F, Session[F], Unit] =
    Kleisli(_.execute(sql).void)
}
