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

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.triplesstore.DatasetTTLs
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.Migration
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling.RecoverableErrorsRecovery
import io.renku.triplesstore._
import io.renku.triplesstore.TSAdminClient.CreationResult
import org.typelevel.log4cats.Logger

private trait DatasetsCreator[F[_]] extends Migration[F]

private object DatasetsCreator {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[Migration[F]] = for {
    tsAdminClient  <- TSAdminClient[F]
    datasetConfigs <- MonadThrow[F].fromEither(DatasetTTLs.allConfigs)
  } yield new DatasetsCreatorImpl[F](datasetConfigs, tsAdminClient)
}

private class DatasetsCreatorImpl[F[_]: MonadThrow: Logger](
    datasets:         List[(DatasetName, DatasetConfigFile)],
    tsAdminClient:    TSAdminClient[F],
    recoveryStrategy: RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends DatasetsCreator[F] {

  // In fact this is an exclusive migration but not Registered
  // Hence, the flag has to be set to false to satisfy Migrations validation
  // TSStateChecker returns a special state if it's ongoing or not run
  override val exclusive: Boolean        = false
  override val name:      Migration.Name = Migration.Name("Datasets creation")

  import recoveryStrategy._
  import tsAdminClient._

  override def run(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    datasets
      .map { case (datasetName, datasetConfig) =>
        createDataset(datasetConfig) >>= logSuccess(datasetName)
      }
      .sequence
      .void
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }

  private lazy val logSuccess: DatasetName => CreationResult => F[Unit] =
    datasetName => result => Logger[F].info(show"$categoryName: $name -> '$datasetName' $result")
}
