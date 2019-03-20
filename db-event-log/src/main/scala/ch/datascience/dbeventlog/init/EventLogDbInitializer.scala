/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.dbeventlog.init

import cats.MonadError
import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.db.TransactorProvider
import ch.datascience.dbeventlog.IOTransactorProvider
import ch.datascience.logging.ApplicationLogger
import doobie.util.fragment.Fragment
import doobie.util.transactor.Transactor.Aux
import io.chrisdavenport.log4cats.Logger

import scala.language.higherKinds
import scala.util.control.NonFatal

class EventLogDbInitializer[Interpretation[_]](
    transactorProvider: TransactorProvider[Interpretation],
    logger:             Logger[Interpretation]
)(implicit ME:          MonadError[Interpretation, Throwable]) {

  import doobie.implicits._

  def run: Interpretation[Unit] = {
    for {
      transactor <- transactorProvider.transactor
      _          <- createTable(transactor)
      _          <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_id ON event_log(project_id)", transactor)
      _          <- execute(sql"CREATE INDEX IF NOT EXISTS idx_status ON event_log(status)", transactor)
      _          <- execute(sql"CREATE INDEX IF NOT EXISTS idx_execution_date ON event_log(execution_date DESC)", transactor)
      _          <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_date ON event_log(event_date DESC)", transactor)
      _          <- logger.info("Event Log database initialization success")
    } yield ()
  } recoverWith logging

  private def createTable(transactor: Aux[Interpretation, Unit]): Interpretation[Unit] =
    sql"""
         |CREATE TABLE IF NOT EXISTS event_log(
         | event_id varchar PRIMARY KEY,
         | project_id int4 NOT NULL,
         | status varchar NOT NULL,
         | created_date timestamp NOT NULL,
         | execution_date timestamp NOT NULL,
         | event_date timestamp NOT NULL,
         | event_body text NOT NULL,
         | message varchar
         |);
       """.stripMargin.update.run
      .transact(transactor)
      .map(_ => ())

  private def execute(sql: Fragment, transactor: Aux[Interpretation, Unit]): Interpretation[Unit] =
    sql.update.run
      .transact(transactor)
      .map(_ => ())

  private lazy val logging: PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)("Event Log database initialization failure")
      ME.raiseError(exception)
  }
}

class IOEventLogDbInitializer(implicit contextShift: ContextShift[IO])
    extends EventLogDbInitializer[IO](
      transactorProvider = new IOTransactorProvider,
      logger             = ApplicationLogger
    )
