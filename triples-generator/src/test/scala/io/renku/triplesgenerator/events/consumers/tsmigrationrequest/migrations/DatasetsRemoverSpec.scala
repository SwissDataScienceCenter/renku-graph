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
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling.RecoverableErrorsRecovery
import io.renku.triplesgenerator.generators.ErrorGenerators.processingRecoverableErrors
import io.renku.triplesstore.{DatasetName, TSAdminClient}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class DatasetsRemoverSpec extends AnyWordSpec with should.Matchers with MockFactory with TableDrivenPropertyChecks {

  "run" should {

    TSAdminClient.RemovalResult.Removed :: TSAdminClient.RemovalResult.NotExisted :: Nil foreach { result =>
      s"succeed when 'renku' dataset removal has finished with $result" in new TestCase {

        givenDSRemoval(of = renkuDataset, returning = result.pure[Try])

        dsRemover.run().value shouldBe ().asRight.pure[Try]

        logger.loggedOnly(Info(show"$categoryName: ${dsRemover.name} -> 'renku' $result"))
      }
    }

    "return a Recoverable Error if in case of an exception the given strategy returns one" in new TestCase {

      val exception = exceptions.generateOne
      givenDSRemoval(of = renkuDataset, returning = exception.raiseError[Try, TSAdminClient.RemovalResult])

      dsRemover.run().value shouldBe recoverableError.asLeft.pure[Try]
    }
  }

  private trait TestCase {
    val renkuDataset = DatasetName("renku")

    val recoverableError = processingRecoverableErrors.generateOne
    val recoveryStrategy = new RecoverableErrorsRecovery {
      override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
        recoverableError.asLeft[OUT].pure[F]
      }
    }

    implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val tsAdminClient  = mock[TSAdminClient[Try]]
    lazy val dsRemover = new DatasetsRemoverImpl[Try](tsAdminClient, recoveryStrategy)

    def givenDSRemoval(of: DatasetName, returning: Try[TSAdminClient.RemovalResult]) =
      (tsAdminClient.removeDataset _).expects(of).returning(returning)
  }
}
