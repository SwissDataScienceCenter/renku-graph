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
import cats.syntax.all._
import ch.datascience.db.SessionResource
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB
import skunk._
import skunk.implicits._
import skunk.codec.all._

private trait EventLogTableRenamer[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object EventLogTableRenamer {
  def apply[Interpretation[_]: Async: Bracket[*[_], Throwable]](
      transactor: SessionResource[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): EventLogTableRenamer[Interpretation] =
    new EventLogTableRenamerImpl(transactor, logger)
}

private class EventLogTableRenamerImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    transactor: SessionResource[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends EventLogTableRenamer[Interpretation]
    with EventTableCheck {

  override def run(): Interpretation[Unit] = transactor.use { implicit session =>
    checkOldTableExists flatMap {
      case false => logger info "'event' table already exists"
      case true =>
        whenEventTableExists(
          dropOldTable(),
          otherwise = renameTable()
        )
    }
  }

  private def checkOldTableExists(implicit session: Session[Interpretation]): Interpretation[Boolean] = {
    val query: Query[Void, Boolean] = sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'event_log')"
      .query(bool)
    session.unique(query).recover { case _ => false }
  }

  private def renameTable()(implicit session: Session[Interpretation]) = for {
    _ <- execute(sql"ALTER TABLE event_log RENAME TO event".command)
    _ <- logger info "'event_log' table renamed to 'event'"
  } yield ()

  private def dropOldTable()(implicit session: Session[Interpretation]) = for {
    _ <- execute(sql"DROP TABLE IF EXISTS event_log".command)
    _ <- logger info "'event_log' table dropped"
  } yield ()
}
