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

package ch.datascience.tokenrepository.repository.init

import cats.MonadError
import cats.effect.{ContextShift, ExitCode, IO}
import cats.implicits._
import ch.datascience.db.TransactorProvider
import ch.datascience.logging.ApplicationLogger
import ch.datascience.tokenrepository.repository.ProjectsTokensConfig
import doobie.util.transactor.Transactor.Aux
import io.chrisdavenport.log4cats.Logger

import scala.language.higherKinds
import scala.util.control.NonFatal

class DbInitializer[Interpretation[_]](
    transactorProvider: TransactorProvider[Interpretation],
    logger:             Logger[Interpretation]
)(implicit ME:          MonadError[Interpretation, Throwable]) {

  import doobie.implicits._

  def run: Interpretation[ExitCode] = {
    for {
      transactor <- transactorProvider.transactor
      _          <- createTable(transactor)
    } yield {
      logger.info("Database initialization success")
      ExitCode.Success
    }
  } recoverWith logging

  private def createTable(transactor: Aux[Interpretation, Unit]): Interpretation[Unit] =
    sql"""
         |CREATE TABLE IF NOT EXISTS projects_tokens(
         | project_id int4 PRIMARY KEY,
         | token VARCHAR NOT NULL
         |);
       """.stripMargin.update.run
      .transact(transactor)
      .map(_ => ())

  private lazy val logging: PartialFunction[Throwable, Interpretation[ExitCode]] = {
    case NonFatal(exception) =>
      logger.error(exception)("Database initialization failure")
      ME.raiseError(exception)
  }
}

class IODbInitializer(implicit contextShift: ContextShift[IO])
    extends DbInitializer[IO](
      transactorProvider = new TransactorProvider[IO](new ProjectsTokensConfig[IO]),
      logger             = ApplicationLogger
    )
