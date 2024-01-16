/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest

import cats.MonadThrow
import cats.data.EitherT
import cats.data.EitherT.{liftF, right}
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.Config
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import io.renku.triplesstore.SparqlQueryTimeRecorder
import migrations.Migrations
import migrations.reprovisioning.ReProvisioningStatus
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait MigrationsRunner[F[_]] {
  def run(): EitherT[F, ProcessingRecoverableError, Unit]
}

private class MigrationsRunnerImpl[F[_]: MonadThrow: Logger](migrations: List[Migration[F]])
    extends MigrationsRunner[F] {

  override def run(): EitherT[F, ProcessingRecoverableError, Unit] =
    migrations.foldLeft(liftF[F, ProcessingRecoverableError, Unit](().pure[F]))(_ >> run(_))

  private def run(migration: Migration[F]) = EitherT {
    right[ProcessingRecoverableError](Logger[F].info(show"$categoryName: ${migration.name} starting"))
      .flatMap(_ => migration.run())
      .semiflatMap(_ => Logger[F].info(show"$categoryName: ${migration.name} done"))
      .leftSemiflatTap(logError(migration))
      .value
      .recoverWith(errorInLogs(migration))
  }

  private def logError(migration: Migration[F]): ProcessingRecoverableError => F[Unit] = { recoverableFailure =>
    Logger[F].error(recoverableFailure.cause)(
      show"$categoryName: ${migration.name} failed: ${recoverableFailure.message}"
    )
  }

  private def errorInLogs(
      migration: Migration[F]
  ): PartialFunction[Throwable, F[Either[ProcessingRecoverableError, Unit]]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(show"$categoryName: ${migration.name} failed") >>
      exception.raiseError[F, Either[ProcessingRecoverableError, Unit]]
  }
}

private object MigrationsRunner {
  def apply[F[_]: Async: ReProvisioningStatus: Logger: MetricsRegistry: SparqlQueryTimeRecorder](
      config: Config
  ): F[MigrationsRunner[F]] = Migrations[F](config).map(new MigrationsRunnerImpl[F](_))
}
