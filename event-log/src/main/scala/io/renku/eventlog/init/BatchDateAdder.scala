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
import cats.effect.BracketThrow
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.eventlog.EventLogDB
import io.renku.graph.model.events.BatchDate
import org.typelevel.log4cats.Logger
import skunk._
import skunk.codec.all.timestamp
import skunk.implicits._

import java.time.{LocalDateTime, ZoneOffset}
import scala.util.control.NonFatal

private trait BatchDateAdder[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object BatchDateAdder {
  def apply[Interpretation[_]: BracketThrow: Logger](
      sessionResource: SessionResource[Interpretation, EventLogDB]
  ): BatchDateAdder[Interpretation] =
    new BatchDateAdderImpl(sessionResource)
}

private class BatchDateAdderImpl[Interpretation[_]: BracketThrow: Logger](
    sessionResource: SessionResource[Interpretation, EventLogDB]
) extends BatchDateAdder[Interpretation]
    with EventTableCheck {

  private implicit val transact: SessionResource[Interpretation, EventLogDB] = sessionResource

  override def run(): Interpretation[Unit] = sessionResource.useK {
    whenEventTableExists(
      Kleisli.liftF(Logger[Interpretation] info "'batch_date' column adding skipped"),
      otherwise = checkColumnExists >>= {
        case true  => Kleisli.liftF(Logger[Interpretation] info "'batch_date' column exists")
        case false => addColumn()
      }
    )
  }

  private lazy val checkColumnExists: Kleisli[Interpretation, Session[Interpretation], Boolean] = {
    val query: Query[skunk.Void, BatchDate] = sql"SELECT batch_date FROM event_log limit 1"
      .query(timestamp)
      .map { case time: LocalDateTime => BatchDate(time.toInstant(ZoneOffset.UTC)) }
    Kleisli(
      _.option(query)
        .map(_ => true)
        .recover { case _ => false }
    )
  }

  private def addColumn(): Kleisli[Interpretation, Session[Interpretation], Unit] = {
    for {
      _ <- execute(sql"ALTER TABLE event_log ADD COLUMN IF NOT EXISTS batch_date timestamp".command)
      _ <- execute(sql"update event_log set batch_date = created_date".command)
      _ <- execute(sql"ALTER TABLE event_log ALTER COLUMN batch_date SET NOT NULL".command)
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_batch_date ON event_log(batch_date)".command)
      _ <- Kleisli.liftF(Logger[Interpretation].info("'batch_date' column added"))
    } yield ()
  } recoverWith logging

  private lazy val logging: PartialFunction[Throwable, Kleisli[Interpretation, Session[Interpretation], Unit]] = {
    case NonFatal(exception) =>
      Logger[Interpretation].error(exception)("'batch_date' column adding failure")
      Kleisli.liftF(exception.raiseError[Interpretation, Unit])
  }
}
