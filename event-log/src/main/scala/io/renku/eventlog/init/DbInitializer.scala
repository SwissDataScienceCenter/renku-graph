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

import cats.effect.{Bracket, ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.DbTransactor
import ch.datascience.graph.model.events.EventStatus.RecoverableFailure
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

import scala.util.control.NonFatal

trait DbInitializer[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

class DbInitializerImpl[Interpretation[_]](
    projectPathAdder:            ProjectPathAdder[Interpretation],
    batchDateAdder:              BatchDateAdder[Interpretation],
    latestEventDatesViewCreator: LatestEventDatesViewCreator[Interpretation],
    projectTableCreator:         ProjectTableCreator[Interpretation],
    transactor:                  DbTransactor[Interpretation, EventLogDB],
    logger:                      Logger[Interpretation]
)(implicit ME:                   Bracket[Interpretation, Throwable])
    extends DbInitializer[Interpretation] {

  import doobie.implicits._
  private implicit val transact: DbTransactor[Interpretation, EventLogDB] = transactor

  override def run(): Interpretation[Unit] = {
    for {
      _ <- createTable()
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_id ON event_log(project_id)")
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_id ON event_log(event_id)")
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_status ON event_log(status)")
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_execution_date ON event_log(execution_date DESC)")
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_date ON event_log(event_date DESC)")
      _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_created_date ON event_log(created_date DESC)")
      _ <- execute(sql"UPDATE event_log set status=${RecoverableFailure.value} where status='TRIPLES_STORE_FAILURE'")
      _ <- logger.info("Event Log database initialization success")
      _ <- projectPathAdder.run()
      _ <- batchDateAdder.run()
      _ <- latestEventDatesViewCreator.run()
      _ <- projectTableCreator.run()
    } yield ()
  } recoverWith logging

  private def createTable(): Interpretation[Unit] =
    sql"""
         |CREATE TABLE IF NOT EXISTS event_log(
         | event_id varchar NOT NULL,
         | project_id int4 NOT NULL,
         | status varchar NOT NULL,
         | created_date timestamp NOT NULL,
         | execution_date timestamp NOT NULL,
         | event_date timestamp NOT NULL,
         | event_body text NOT NULL,
         | message varchar,
         | PRIMARY KEY (event_id, project_id)
         |);
       """.stripMargin.update.run
      .transact(transactor.get)
      .map(_ => ())

  private lazy val logging: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)("Event Log database initialization failure")
    ME.raiseError(exception)
  }
}

object IODbInitializer {
  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      logger:              Logger[IO]
  )(implicit contextShift: ContextShift[IO]): IO[DbInitializer[IO]] = IO {
    new DbInitializerImpl[IO](
      new ProjectPathAdder[IO](transactor, logger),
      new BatchDateAdder[IO](transactor, logger),
      LatestEventDatesViewCreator[IO](transactor, logger),
      ProjectTableCreator(transactor, logger),
      transactor,
      logger
    )
  }
}
