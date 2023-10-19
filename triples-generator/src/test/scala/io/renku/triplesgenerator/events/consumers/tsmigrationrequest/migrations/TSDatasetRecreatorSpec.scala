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

import Generators.migrationNames
import cats.MonadThrow
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.datasetConfigFiles
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.errors.ErrorGenerators.processingRecoverableErrors
import io.renku.triplesstore.{DatasetConfigFile, DatasetName, TSAdminClient}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.EitherValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import tooling.{MigrationExecutionRegister, RecoverableErrorsRecovery, RegisteredMigration}

class TSDatasetRecreatorSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with should.Matchers
    with AsyncMockFactory
    with EitherValues {

  it should "be a registered migration" in {
    migration.getClass.getSuperclass shouldBe classOf[RegisteredMigration[IO]]
  }

  it should "delete and create the selected Dataset" in {

    (tsAdminClient.removeDataset _)
      .expects(datasetName)
      .returning(TSAdminClient.RemovalResult.Removed.pure[IO])

    (tsAdminClient.createDataset _)
      .expects(datasetFile)
      .returning(TSAdminClient.CreationResult.Created.pure[IO])

    migration.migrate().value.asserting(_.value shouldBe ())
  }

  private val migrationName = migrationNames.generateOne
  private lazy val datasetName: DatasetName       = nonEmptyStrings().generateAs(DatasetName)
  private lazy val datasetFile: DatasetConfigFile = datasetConfigFiles.generateOne

  private implicit val logger: TestLogger[IO] = TestLogger[IO]()
  private lazy val tsAdminClient    = mock[TSAdminClient[IO]]
  private val executionRegister     = mock[MigrationExecutionRegister[IO]]
  private lazy val recoverableError = processingRecoverableErrors.generateOne
  private val recoveryStrategy = new RecoverableErrorsRecovery {
    override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
      recoverableError.asLeft[OUT].pure[F]
    }
  }
  private lazy val migration = new TSDatasetRecreatorImpl[IO](migrationName,
                                                              datasetName,
                                                              datasetFile,
                                                              tsAdminClient,
                                                              executionRegister,
                                                              recoveryStrategy
  )
}
