/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import io.renku.triplesstore.TSAdminClient.{CreationResult, RemovalResult}
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger
import tooling.{MigrationExecutionRegister, RecoverableErrorsRecovery, RegisteredMigration}

private trait TSDatasetRecreator[F[_]] extends Migration[F]

private object TSDatasetRecreator {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder, TT <: DatasetConfigFile](
      suffix:          String,
      dsConfigFactory: DatasetConfigFileFactory[TT]
  ): F[Migration[F]] =
    (MonadThrow[F].fromEither(dsConfigFactory.fromTtlFile()), TSAdminClient[F], MigrationExecutionRegister[F])
      .mapN((ttlFile, tsAdmin, execRegister) =>
        new TSDatasetRecreatorImpl[F](Migration.Name(show"Recreate ${dsConfigFactory.datasetName} dataset $suffix"),
                                      dsConfigFactory.datasetName,
                                      ttlFile,
                                      tsAdmin,
                                      execRegister
        )
      )
}

private class TSDatasetRecreatorImpl[F[_]: Async: Logger](
    migrationName:     Migration.Name,
    datasetName:       DatasetName,
    datasetFile:       DatasetConfigFile,
    tsAdminClient:     TSAdminClient[F],
    executionRegister: MigrationExecutionRegister[F],
    recoveryStrategy:  RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends RegisteredMigration[F](migrationName, executionRegister, recoveryStrategy)
    with TSDatasetRecreator[F] {

  import recoveryStrategy.maybeRecoverableError

  protected[migrations] override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    tsAdminClient
      .removeDataset(datasetName)
      .flatMap {
        case RemovalResult.Removed | RemovalResult.NotExisted => createDS()
        case result => Logger[F].error(show"$categoryName: $name DS not removed: $result")
      }
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }

  private def createDS() =
    tsAdminClient.createDataset(datasetFile) >>= {
      case CreationResult.Created => Logger[F].info(show"$categoryName: $name DS $datasetName re-created")
      case result                 => Logger[F].error(show"$categoryName: $name DS not created: $result")
    }
}
