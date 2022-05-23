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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest
package migrations
package tooling

import Generators.migrationNames
import cats.MonadThrow
import cats.data.EitherT
import cats.syntax.all._
import io.renku.config.ServiceVersion
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.tsmigrationrequest.ConditionedMigration.MigrationRequired
import io.renku.triplesgenerator.generators.ErrorGenerators.processingRecoverableErrors
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class RegisteredMigrationSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "required" should {

    "return Yes if Migration Execution Register cannot find any past executions" in new TestCase {
      (executionRegister.findExecution _)
        .expects(migration.name)
        .returning(None.pure[Try])

      migration.required.value shouldBe MigrationRequired.Yes("was not executed yet").asRight.pure[Try]
    }

    "return No if Migration Execution Register finds a past version" in new TestCase {
      val version = serviceVersions.generateOne
      (executionRegister.findExecution _)
        .expects(migration.name)
        .returning(version.some.pure[Try])

      migration.required.value shouldBe MigrationRequired.No(s"was executed on $version").asRight.pure[Try]
    }

    "return a Recoverable Error if in case of an exception the given strategy returns one" in new TestCase {
      val exception = exceptions.generateOne
      (executionRegister.findExecution _)
        .expects(migration.name)
        .returning(exception.raiseError[Try, Option[ServiceVersion]])

      migration.required.value shouldBe recoverableError.asLeft.pure[Try]
    }
  }

  "postMigration" should {

    "update the Execution Register" in new TestCase {

      (executionRegister.registerExecution _)
        .expects(migration.name)
        .returning(().pure[Try])

      migration.postMigration().value shouldBe ().asRight.pure[Try]
    }

    "return a Recoverable Error if in case of an exception " +
      "the given strategy returns one" in new TestCase {
        val exception = exceptions.generateOne
        (executionRegister.registerExecution _)
          .expects(migration.name)
          .returning(exception.raiseError[Try, Unit])

        migration.postMigration().value shouldBe recoverableError.asLeft.pure[Try]
      }
  }

  private trait TestCase {

    private implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val executionRegister = mock[MigrationExecutionRegister[Try]]
    val recoverableError  = processingRecoverableErrors.generateOne
    val recoveryStrategy = new RecoverableErrorsRecovery {
      override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
        recoverableError.asLeft[OUT].pure[F]
      }
    }
    val migration = new RegisteredMigration[Try](migrationNames.generateOne, executionRegister, recoveryStrategy) {

      protected override def migrate(): EitherT[Try, ProcessingRecoverableError, Unit] =
        EitherT.pure[Try, ProcessingRecoverableError](())
    }
  }
}
