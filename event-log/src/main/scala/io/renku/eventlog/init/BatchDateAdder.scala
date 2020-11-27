/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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
import ch.datascience.db.DbTransactor
import doobie.implicits._
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

import scala.util.control.NonFatal

private trait BatchDateAdder[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object BatchDateAdder {
  def apply[Interpretation[_]](
      transactor: DbTransactor[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): BatchDateAdder[Interpretation] =
    new BatchDateAdderImpl(transactor, logger)
}

private class BatchDateAdderImpl[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends BatchDateAdder[Interpretation] {

  private implicit val transact: DbTransactor[Interpretation, EventLogDB] = transactor

  override def run(): Interpretation[Unit] =
    checkColumnExists flatMap {
      case true  => logger.info("'batch_date' column exists")
      case false => addColumn()
    }

  private def checkColumnExists: Interpretation[Boolean] =
    sql"select batch_date from event_log limit 1"
      .query[String]
      .option
      .transact(transactor.get)
      .map(_ => true)
      .recover { case _ => false }

  private def addColumn() = {
    for {
      _ <- execute(sql"ALTER TABLE event_log ADD COLUMN IF NOT EXISTS batch_date timestamp")
      _ <- execute(sql"update event_log set batch_date = created_date")
      _ <- execute(sql"ALTER TABLE event_log ALTER COLUMN batch_date SET NOT NULL")
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_batch_date ON event_log(batch_date)")
      _ <- logger.info("'batch_date' column added")
    } yield ()
  } recoverWith logging

  private lazy val logging: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)("'batch_date' column adding failure")
    ME.raiseError(exception)
  }
}
