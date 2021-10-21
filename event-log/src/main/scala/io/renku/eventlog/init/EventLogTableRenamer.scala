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
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.eventlog.EventLogDB
import org.typelevel.log4cats.Logger
import skunk._
import skunk.codec.all._
import skunk.implicits._

private trait EventLogTableRenamer[F[_]] {
  def run(): F[Unit]
}

private object EventLogTableRenamer {
  def apply[F[_]: MonadCancelThrow: Logger](
      sessionResource: SessionResource[F, EventLogDB]
  ): EventLogTableRenamer[F] = new EventLogTableRenamerImpl(sessionResource)
}

private class EventLogTableRenamerImpl[F[_]: MonadCancelThrow: Logger](
    sessionResource: SessionResource[F, EventLogDB]
) extends EventLogTableRenamer[F]
    with EventTableCheck {

  override def run(): F[Unit] = sessionResource.useK {
    checkOldTableExists >>= {
      case false => Kleisli.liftF(Logger[F] info "'event' table already exists")
      case true =>
        whenEventTableExists(
          dropOldTable(),
          otherwise = renameTable()
        )
    }
  }

  private lazy val checkOldTableExists: Kleisli[F, Session[F], Boolean] = {
    val query: Query[Void, Boolean] = sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'event_log')"
      .query(bool)
    Kleisli(_.unique(query).recover { case _ => false })
  }

  private def renameTable(): Kleisli[F, Session[F], Unit] = for {
    _ <- execute(sql"ALTER TABLE event_log RENAME TO event".command)
    _ <- Kleisli.liftF(Logger[F] info "'event_log' table renamed to 'event'")
  } yield ()

  private def dropOldTable(): Kleisli[F, Session[F], Unit] = for {
    _ <- execute(sql"DROP TABLE IF EXISTS event_log".command)
    _ <- Kleisli.liftF(Logger[F] info "'event_log' table dropped")
  } yield ()
}
