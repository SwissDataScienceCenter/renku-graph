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
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling.RecoverableErrorsRecovery
import io.renku.triplesgenerator.errors.ErrorGenerators.processingRecoverableErrors
import io.renku.triplesstore.{DatasetName, TSAdminClient}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class DatasetsRemoverSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with MockFactory
    with TableDrivenPropertyChecks {

  "run" should {

    TSAdminClient.RemovalResult.Removed :: TSAdminClient.RemovalResult.NotExisted :: Nil foreach { result =>
      s"succeed when 'renku' dataset removal has finished with $result" in new TestCase {

        givenDSRemoval(of = renkuDataset, returning = result.pure[IO])

        dsRemover.run().value.unsafeRunSync() shouldBe ().asRight

        logger.loggedOnly(Info(show"$categoryName: ${dsRemover.name} -> 'renku' $result"))
      }
    }

    "return a Recoverable Error if in case of an exception the given strategy returns one" in new TestCase {

      val exception = exceptions.generateOne
      givenDSRemoval(of = renkuDataset, returning = exception.raiseError[IO, TSAdminClient.RemovalResult])

      dsRemover.run().value.unsafeRunSync() shouldBe recoverableError.asLeft
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

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val tsAdminClient  = mock[TSAdminClient[IO]]
    lazy val dsRemover = new DatasetsRemoverImpl[IO](tsAdminClient, recoveryStrategy)

    def givenDSRemoval(of: DatasetName, returning: IO[TSAdminClient.RemovalResult]) =
      (tsAdminClient.removeDataset _).expects(of).returning(returning)
  }
}
