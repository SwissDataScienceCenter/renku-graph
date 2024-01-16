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
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.Config
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus
import io.renku.triplesstore.SparqlQueryTimeRecorder
import migrations.Migrations
import migrations.tooling.MigrationExecutionRegister
import org.typelevel.log4cats.Logger

trait MigrationStatusChecker[F[_]] {
  def underMigration: F[Boolean]
}

object MigrationStatusChecker {
  def apply[F[_]: Async: ReProvisioningStatus: Logger: MetricsRegistry: SparqlQueryTimeRecorder](
      config: Config
  ): F[MigrationStatusChecker[F]] =
    (MigrationExecutionRegister[F] -> Migrations[F](config))
      .mapN(new MigrationStatusCheckerImpl[F](_, _))
}

private class MigrationStatusCheckerImpl[F[_]: MonadThrow](executionRegister: MigrationExecutionRegister[F],
                                                           migrations: List[Migration[F]]
) extends MigrationStatusChecker[F] {

  override def underMigration: F[Boolean] =
    findLastExclusiveMigration match {
      case None    => false.pure[F]
      case Some(m) => wasRun(m)
    }

  private lazy val findLastExclusiveMigration: Option[Migration[F]] =
    migrations.findLast(_.exclusive)

  private def wasRun(migration: Migration[F]): F[Boolean] =
    executionRegister.findExecution(migration.name).map(_.isEmpty)
}
