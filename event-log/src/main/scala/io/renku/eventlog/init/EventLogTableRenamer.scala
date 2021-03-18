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
import cats.syntax.all._
import ch.datascience.db.SessionResource
import doobie.implicits._
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

private trait EventLogTableRenamer[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object EventLogTableRenamer {
  def apply[Interpretation[_]](
      transactor: SessionResource[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): EventLogTableRenamer[Interpretation] =
    new EventLogTableRenamerImpl(transactor, logger)
}

private class EventLogTableRenamerImpl[Interpretation[_]](
    transactor: SessionResource[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends EventLogTableRenamer[Interpretation]
    with EventTableCheck[Interpretation] {

  private implicit val transact: SessionResource[Interpretation, EventLogDB] = transactor

  override def run(): Interpretation[Unit] =
    checkOldTableExists flatMap {
      case false => logger info "'event' table already exists"
      case true =>
        whenEventTableExists(
          dropOldTable(),
          otherwise = renameTable()
        )
    }

  private def checkOldTableExists: Interpretation[Boolean] =
    sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'event_log')"
      .query[Boolean]
      .unique
      .transact(transactor.resource)
      .recover { case _ => false }

  private def renameTable() = for {
    _ <- execute(sql"ALTER TABLE event_log RENAME TO event")
    _ <- logger info "'event_log' table renamed to 'event'"
  } yield ()

  private def dropOldTable() = for {
    _ <- execute(sql"DROP TABLE IF EXISTS event_log")
    _ <- logger info "'event_log' table dropped"
  } yield ()
}
