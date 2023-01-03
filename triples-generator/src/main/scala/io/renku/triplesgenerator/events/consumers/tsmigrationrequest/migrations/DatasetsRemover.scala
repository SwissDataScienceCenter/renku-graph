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
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.Migration
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling.RecoverableErrorsRecovery
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private trait DatasetsRemover[F[_]] extends Migration[F]

private object DatasetsRemover {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[Migration[F]] =
    TSAdminClient[F].map(new DatasetsRemoverImpl[F](_))
}

private class DatasetsRemoverImpl[F[_]: MonadThrow: Logger](
    tsAdminClient:    TSAdminClient[F],
    recoveryStrategy: RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends DatasetsCreator[F] {

  override lazy val name = Migration.Name("Datasets remover")

  import recoveryStrategy._
  import tsAdminClient._

  private val datasetName = DatasetName("renku")

  override def run(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    (removeDataset(datasetName) >>= logSuccess)
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }

  private lazy val logSuccess: TSAdminClient.RemovalResult => F[Unit] = result =>
    Logger[F].info(show"$categoryName: $name -> '$datasetName' $result")
}
