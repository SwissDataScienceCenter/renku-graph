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

package ch.datascience.tokenrepository.repository.init

import cats.effect._
import cats.syntax.all._
import ch.datascience.db.{SessionResource, SqlQuery}
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.tokenrepository.repository.ProjectsTokensDB
import io.chrisdavenport.log4cats.Logger
import skunk.data.Completion

import scala.util.control.NonFatal

class DbInitializer[Interpretation[_]](
    projectPathAdder:         ProjectPathAdder[Interpretation],
    duplicateProjectsRemover: DuplicateProjectsRemover[Interpretation],
    transactor:               SessionResource[Interpretation, ProjectsTokensDB],
    logger:                   Logger[Interpretation]
)(implicit ME:                Bracket[Interpretation, Throwable]) {

  import skunk._
  import skunk.implicits._

  def run(): Interpretation[Unit] = {
    for {
      _ <- createTable
      _ <- projectPathAdder.run()
      _ <- duplicateProjectsRemover.run()
      _ <- logger.info("Projects Tokens database initialization success")
    } yield ()
  } recoverWith logging

  private def createTable: Interpretation[Unit] =
    transactor
      .use { session =>
        session.transaction.use { xa =>
          val query: Command[Void] = sql"""CREATE TABLE IF NOT EXISTS projects_tokens(
                            project_id int4 PRIMARY KEY,
                            token VARCHAR NOT NULL
                            );""".command
          for {
            sp <- xa.savepoint
            _ <- session.execute(query).recoverWith { case e =>
                   xa.rollback(sp).flatMap(_ => e.raiseError[Interpretation, Completion])
                 }
          } yield ()
        }
      }

  private lazy val logging: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)("Projects Tokens database initialization failure")
    ME.raiseError(exception)
  }
}

object IODbInitializer {
  import scala.concurrent.ExecutionContext

  def apply(
      transactor:       SessionResource[IO, ProjectsTokensDB],
      queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
      logger:           Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[DbInitializer[IO]] =
    for {
      pathAdder <- IOProjectPathAdder(transactor, queriesExecTimes, logger)
      duplicateProjectsRemover = DuplicateProjectsRemover[IO](transactor, logger)
    } yield new DbInitializer[IO](pathAdder, duplicateProjectsRemover, transactor, logger)
}
