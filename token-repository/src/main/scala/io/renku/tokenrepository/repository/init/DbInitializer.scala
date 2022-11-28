/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.effect._
import cats.syntax.all._
import io.renku.http.client.GitLabClient
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.ProjectsTokensDB.SessionResource
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.control.NonFatal

trait DbInitializer[F[_]] {
  def run(): F[Unit]
}

class DbInitializerImpl[F[_]: Async: Logger](migrators: List[DBMigration[F]],
                                             retrySleepDuration: FiniteDuration = 20 seconds
) extends DbInitializer[F] {

  override def run(): F[Unit] = {
    for {
      _ <- migrators.map(_.run()).sequence
      _ <- Logger[F].info("Projects Tokens database initialization success")
    } yield ()
  } recoverWith logAndRetry

  private def logAndRetry: PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    for {
      _ <- Temporal[F] sleep retrySleepDuration
      _ <- Logger[F].error(exception)(s"Projects Tokens database initialization failed")
      _ <- run()
    } yield ()
  }
}

object DbInitializer {

  def migrations[F[_]: Async: GitLabClient: Logger: SessionResource](
      queriesExecTimes: LabeledHistogram[F]
  ): F[List[DBMigration[F]]] = List(
    ProjectsTokensTableCreator[F].pure[F],
    ProjectPathAdder[F].pure[F],
    DuplicateProjectsRemover[F].pure[F],
    ExpiryAndCreatedDatesAdder[F].pure[F],
    TokensMigrator[F](queriesExecTimes)
  ).sequence

  def apply[F[_]: Async: GitLabClient: Logger: SessionResource](
      queriesExecTimes: LabeledHistogram[F]
  ): F[DbInitializer[F]] = migrations[F](queriesExecTimes).map(new DbInitializerImpl[F](_))
}
