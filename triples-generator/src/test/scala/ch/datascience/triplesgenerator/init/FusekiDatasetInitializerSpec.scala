/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.init

import cats.MonadError
import cats.implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators
import ServiceTypesGenerators._
import ch.datascience.generators.Generators.Implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.language.postfixOps
import scala.util.Try

class FusekiDatasetInitializerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {

    "succeed if the relevant dataset exists" in new TestCase {

      (datasetExistenceChecker.doesDatasetExists _)
        .expects()
        .returning(context.pure(true))

      datasetVerifier.run shouldBe context.unit

      logger.loggedOnly(Info(s"'${fusekiConfig.datasetName}' dataset exists in Jena; No action needed."))
    }

    "succeed if the relevant dataset does not exist but was successfully created" in new TestCase {

      (datasetExistenceChecker.doesDatasetExists _)
        .expects()
        .returning(context.pure(false))

      (datasetExistenceCreator.createDataset _)
        .expects()
        .returning(context.unit)

      datasetVerifier.run shouldBe context.unit

      logger.loggedOnly(Info(s"'${fusekiConfig.datasetName}' dataset created in Jena"))
    }

    "fail if check of dataset existence fails" in new TestCase {

      val exception = exceptions.generateOne
      (datasetExistenceChecker.doesDatasetExists _)
        .expects()
        .returning(context.raiseError(exception))

      datasetVerifier.run shouldBe context.raiseError(exception)

      logger.loggedOnly(Error(s"'${fusekiConfig.datasetName}' dataset initialization in Jena failed", exception))
    }

    "fail if dataset creation fails" in new TestCase {

      (datasetExistenceChecker.doesDatasetExists _)
        .expects()
        .returning(context.pure(false))

      val exception = exceptions.generateOne
      (datasetExistenceCreator.createDataset _)
        .expects()
        .returning(context.raiseError(exception))

      datasetVerifier.run shouldBe context.raiseError(exception)

      logger.loggedOnly(Error(s"'${fusekiConfig.datasetName}' dataset initialization in Jena failed", exception))
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val fusekiConfig = fusekiAdminConfigs.generateOne

    val datasetExistenceChecker = mock[TryDatasetExistenceChecker]
    val datasetExistenceCreator = mock[TryDatasetExistenceCreator]
    val logger                  = TestLogger[Try]()
    val datasetVerifier = new FusekiDatasetInitializer[Try](
      fusekiConfig,
      datasetExistenceChecker,
      datasetExistenceCreator,
      logger
    )
  }
}
