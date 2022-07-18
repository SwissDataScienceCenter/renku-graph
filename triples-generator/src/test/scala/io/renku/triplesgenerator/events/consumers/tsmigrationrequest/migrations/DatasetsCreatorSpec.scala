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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations

import cats.MonadThrow
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.datasetConfigFiles
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.triplesstore.RdfStoreAdminClient.CreationResult
import io.renku.triplesstore.{DatasetConfigFile, DatasetName, RdfStoreAdminClient}
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling.RecoverableErrorsRecovery
import io.renku.triplesgenerator.generators.ErrorGenerators.processingRecoverableErrors
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class DatasetsCreatorSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "createDatasets" should {

    "create all passed datasets" in new TestCase {
      val results = datasets map { case (datasetName, datasetConfig) =>
        val result = creationResults.generateOne
        givenDSCreation(of = datasetConfig, returning = result.pure[Try])
        datasetName -> result
      }

      dsCreator.run().value shouldBe ().asRight.pure[Try]

      val logEntries = results map { case (datasetName, result) =>
        Info(show"$categoryName: ${dsCreator.name} -> '$datasetName' $result")
      }
      logger.loggedOnly(logEntries)
    }

    "return a Recoverable Error if in case of an exception the given strategy returns one" in new TestCase {
      val datasetName            = nonEmptyStrings().generateAs(DatasetName)
      val datasetConfig          = datasetConfigFiles.generateOne
      override lazy val datasets = List(datasetName -> datasetConfig)

      val exception = exceptions.generateOne
      givenDSCreation(of = datasetConfig, returning = exception.raiseError[Try, CreationResult])

      dsCreator.run().value shouldBe recoverableError.asLeft.pure[Try]
    }
  }

  private trait TestCase {
    lazy val datasets: List[(DatasetName, DatasetConfigFile)] = (
      for {
        name       <- nonEmptyStrings().toGeneratorOf(DatasetName)
        configFile <- datasetConfigFiles
      } yield name -> configFile
    ).generateNonEmptyList().toList

    val recoverableError = processingRecoverableErrors.generateOne
    val recoveryStrategy = new RecoverableErrorsRecovery {
      override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
        recoverableError.asLeft[OUT].pure[F]
      }
    }

    implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val tsAdminClient  = mock[RdfStoreAdminClient[Try]]
    lazy val dsCreator = new DatasetsCreatorImpl[Try](datasets, tsAdminClient, recoveryStrategy)

    def givenDSCreation(of: DatasetConfigFile, returning: Try[CreationResult]) =
      (tsAdminClient.createDataset _).expects(of).returning(returning)
  }

  private val creationResults: Gen[CreationResult] = Gen.oneOf(CreationResult.Created, CreationResult.Existed)
}
