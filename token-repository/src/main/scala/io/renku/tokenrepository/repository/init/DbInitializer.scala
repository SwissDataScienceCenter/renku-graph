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

import DbInitializer.Runnable
import cats.effect._
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.ProjectsTokensDB
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.language.reflectiveCalls
import scala.util.control.NonFatal

trait DbInitializer[F[_]] {
  def run(): F[Unit]
}

class DbInitializerImpl[F[_]: Async: Logger](migrators: List[Runnable[F, Unit]],
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

  def apply[F[_]: Async: Logger](
      sessionResource:  SessionResource[F, ProjectsTokensDB],
      queriesExecTimes: LabeledHistogram[F]
  ): F[DbInitializer[F]] = for {
    tableCreator             <- ProjectsTokensTableCreator[F](sessionResource).pure[F]
    pathAdder                <- ProjectPathAdder[F](sessionResource, queriesExecTimes)
    duplicateProjectsRemover <- DuplicateProjectsRemover[F](sessionResource).pure[F]
  } yield new DbInitializerImpl[F](List[Runnable[F, Unit]](tableCreator, pathAdder, duplicateProjectsRemover))

  private[init] type Runnable[F[_], R] = { def run(): F[R] }
}
