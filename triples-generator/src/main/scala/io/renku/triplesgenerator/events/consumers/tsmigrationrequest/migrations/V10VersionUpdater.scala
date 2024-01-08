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
package migrations

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import cats.MonadThrow
import com.typesafe.config.ConfigFactory
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.versions.RenkuVersionPair
import io.renku.triplesgenerator.config.VersionCompatibilityConfig
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import io.renku.triplesstore.{MigrationsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger
import reprovisioning.RenkuVersionPairUpdater
import tooling.{MigrationExecutionRegister, RecoverableErrorsRecovery, RegisteredMigration}

private class V10VersionUpdater[F[_]: MonadThrow: Logger](
    versionPair:             RenkuVersionPair,
    renkuVersionPairUpdater: RenkuVersionPairUpdater[F],
    executionRegister:       MigrationExecutionRegister[F],
    recoveryStrategy:        RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends RegisteredMigration[F](V10VersionUpdater.name, executionRegister, recoveryStrategy) {

  override val exclusive: Boolean = true

  import recoveryStrategy._

  protected[migrations] override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    renkuVersionPairUpdater
      .update(versionPair)
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }
}

private[migrations] object V10VersionUpdater {

  val name: Migration.Name = Migration.Name("V10 version Updater")

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[Migration[F]] = for {
    implicit0(ru: RenkuUrl)    <- RenkuUrlLoader[F]()
    config                     <- MonadThrow[F].catchNonFatal(ConfigFactory.load())
    compatibility              <- VersionCompatibilityConfig.fromConfigF[F](config)
    migrationsConnectionConfig <- MigrationsConnectionConfig.fromConfig[F](config)
    executionRegister          <- MigrationExecutionRegister[F]
  } yield new V10VersionUpdater(compatibility.asVersionPair,
                                RenkuVersionPairUpdater(migrationsConnectionConfig),
                                executionRegister
  )
}
