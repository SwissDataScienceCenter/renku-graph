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

package io.renku.tokenrepository.repository.init

import cats.data.Kleisli
import cats.effect._
import cats.effect.kernel.Temporal
import cats.syntax.all._
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.{ProjectsTokensDB, init}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait DbInitializer[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

class DbInitializerImpl[Interpretation[_]: MonadCancelThrow: Logger](
    projectPathAdder:         ProjectPathAdder[Interpretation],
    duplicateProjectsRemover: DuplicateProjectsRemover[Interpretation],
    sessionResource:          SessionResource[Interpretation, ProjectsTokensDB]
) extends DbInitializer[Interpretation] {

  import skunk._
  import skunk.implicits._

  override def run(): Interpretation[Unit] = {
    for {
      _ <- createTable
      _ <- projectPathAdder.run()
      _ <- duplicateProjectsRemover.run()
      _ <- Logger[Interpretation].info("Projects Tokens database initialization success")
    } yield ()
  } recoverWith logging

  private def createTable: Interpretation[Unit] =
    sessionResource.useK {
      val query: Command[Void] =
        sql"""CREATE TABLE IF NOT EXISTS projects_tokens(
                project_id int4 PRIMARY KEY,
                token VARCHAR NOT NULL
              );""".command
      Kleisli[Interpretation, Session[Interpretation], Unit](session => session.execute(query).void)
    }

  private lazy val logging: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    Logger[Interpretation]
      .error(exception)("Projects Tokens database initialization failure")
      .flatMap(_ => MonadCancelThrow[Interpretation].raiseError(exception))
  }
}

object DbInitializer {

  def apply[F[_]: Async: Temporal: Spawn: MonadCancelThrow: Logger](
      sessionResource:  SessionResource[F, ProjectsTokensDB],
      queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ): F[DbInitializer[F]] = for {
    pathAdder <- ProjectPathAdder[F](sessionResource, queriesExecTimes)
    duplicateProjectsRemover = init.DuplicateProjectsRemover[F](sessionResource)
  } yield new DbInitializerImpl[F](pathAdder, duplicateProjectsRemover, sessionResource)
}
