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
package tooling

import Generators.migrationNames
import cats.MonadThrow
import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all._
import io.renku.config.ServiceVersion
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.ConditionedMigration.MigrationRequired
import io.renku.triplesgenerator.errors.ErrorGenerators.processingRecoverableErrors
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class RegisteredMigrationSpec extends AnyWordSpec with IOSpec with should.Matchers with MockFactory {

  "required" should {

    "return Yes if Migration Execution Register cannot find any past executions" in new TestCase {
      (executionRegister.findExecution _)
        .expects(migration.name)
        .returning(None.pure[IO])

      migration.required.value.unsafeRunSync() shouldBe MigrationRequired.Yes("was not executed yet").asRight
    }

    "return No if Migration Execution Register finds a past version" in new TestCase {
      val version = serviceVersions.generateOne
      (executionRegister.findExecution _)
        .expects(migration.name)
        .returning(version.some.pure[IO])

      migration.required.value.unsafeRunSync() shouldBe MigrationRequired.No(s"was executed on $version").asRight
    }

    "return a Recoverable Error if in case of an exception the given strategy returns one" in new TestCase {
      val exception = exceptions.generateOne
      (executionRegister.findExecution _)
        .expects(migration.name)
        .returning(exception.raiseError[IO, Option[ServiceVersion]])

      migration.required.value.unsafeRunSync() shouldBe recoverableError.asLeft
    }
  }

  "postMigration" should {

    "update the Execution Register" in new TestCase {

      (executionRegister.registerExecution _)
        .expects(migration.name)
        .returning(().pure[IO])

      migration.postMigration().value.unsafeRunSync() shouldBe ().asRight
    }

    "return a Recoverable Error if in case of an exception " +
      "the given strategy returns one" in new TestCase {
        val exception = exceptions.generateOne
        (executionRegister.registerExecution _)
          .expects(migration.name)
          .returning(exception.raiseError[IO, Unit])

        migration.postMigration().value.unsafeRunSync() shouldBe recoverableError.asLeft
      }
  }

  private trait TestCase {

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val executionRegister = mock[MigrationExecutionRegister[IO]]
    val recoverableError  = processingRecoverableErrors.generateOne
    val recoveryStrategy = new RecoverableErrorsRecovery {
      override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
        recoverableError.asLeft[OUT].pure[F]
      }
    }
    val migration = new RegisteredMigration[IO](migrationNames.generateOne, executionRegister, recoveryStrategy) {

      protected override def migrate(): EitherT[IO, ProcessingRecoverableError, Unit] =
        EitherT.pure[IO, ProcessingRecoverableError](())
    }
  }
}
