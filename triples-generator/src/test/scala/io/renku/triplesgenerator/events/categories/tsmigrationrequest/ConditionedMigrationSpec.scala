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

import ConditionedMigration.MigrationRequired
import Generators._
import cats.data.EitherT
import cats.data.EitherT._
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.generators.ErrorGenerators.processingRecoverableErrors
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.language.reflectiveCalls
import scala.util.Try

class ConditionedMigrationSpec extends AnyWordSpec with should.Matchers {

  "run" should {

    "log the requirement checking result and do nothing if No" in new TestCase {
      val required  = migrationRequiredNo.generateOne
      val migration = createMigration(required)

      migration.run().value shouldBe ().asRight.pure[Try]

      migration.migrationExecuted     shouldBe false
      migration.postMigrationExecuted shouldBe false

      logger.loggedOnly(Info(show"$categoryName: ${migration.name} $required"))
    }

    "log the requirement checking result and run the migration if Yes" in new TestCase {
      val required  = migrationRequiredYes.generateOne
      val migration = createMigration(required)

      migration.run().value shouldBe ().asRight.pure[Try]

      migration.migrationExecuted     shouldBe true
      migration.postMigrationExecuted shouldBe true

      logger.loggedOnly(Info(show"$categoryName: ${migration.name} $required"))
    }

    "return Processing Recoverable Failure if the migration process returns one" in new TestCase {
      val required         = migrationRequiredYes.generateOne
      val recoverableError = processingRecoverableErrors.generateOne
      val migrationResult  = leftT[Try, Unit](recoverableError)
      val migration        = createMigration(required, migrationResult)

      migration.run() shouldBe migrationResult

      migration.migrationExecuted     shouldBe true
      migration.postMigrationExecuted shouldBe false

      logger.loggedOnly(Info(show"$categoryName: ${migration.name} $required"))
    }

    "fail if the migration process fails" in new TestCase {
      val required        = migrationRequiredYes.generateOne
      val exception       = exceptions.generateOne
      val migrationResult = right[ProcessingRecoverableError](exception.raiseError[Try, Unit])
      val migration       = createMigration(required, migrationResult)

      migration.run().value shouldBe exception.raiseError[Try, Either[ProcessingRecoverableError, Unit]]

      migration.migrationExecuted     shouldBe true
      migration.postMigrationExecuted shouldBe false

      logger.loggedOnly(Info(show"$categoryName: ${migration.name} $required"))
    }

    "return Processing Recoverable Failure if the post migration process returns one" in new TestCase {
      val required            = migrationRequiredYes.generateOne
      val recoverableError    = processingRecoverableErrors.generateOne
      val postMigrationResult = leftT[Try, Unit](recoverableError)
      val migration           = createMigration(required, postMigrationResult = postMigrationResult)

      migration.run() shouldBe postMigrationResult

      migration.migrationExecuted     shouldBe true
      migration.postMigrationExecuted shouldBe true

      logger.loggedOnly(Info(show"$categoryName: ${migration.name} $required"))
    }

    "fail if the post migration process fails" in new TestCase {
      val required            = migrationRequiredYes.generateOne
      val exception           = exceptions.generateOne
      val postMigrationResult = right[ProcessingRecoverableError](exception.raiseError[Try, Unit])
      val migration           = createMigration(required, postMigrationResult = postMigrationResult)

      migration.run().value shouldBe exception.raiseError[Try, Either[ProcessingRecoverableError, Unit]]

      migration.migrationExecuted     shouldBe true
      migration.postMigrationExecuted shouldBe true

      logger.loggedOnly(Info(show"$categoryName: ${migration.name} $required"))
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[Try] = TestLogger[Try]()

    def createMigration(migrationRequired:   MigrationRequired,
                        migrationResult:     EitherT[Try, ProcessingRecoverableError, Unit] = rightT(()),
                        postMigrationResult: EitherT[Try, ProcessingRecoverableError, Unit] = rightT(())
    ) = new ConditionedMigration[Try] {
      override val name     = migrationNames.generateOne
      override val required = rightT(migrationRequired)

      var migrationExecuted: Boolean = false
      protected override def migrate(): EitherT[Try, ProcessingRecoverableError, Unit] = {
        migrationExecuted = true
        migrationResult
      }

      var postMigrationExecuted: Boolean = false
      protected override def postMigration(): EitherT[Try, ProcessingRecoverableError, Unit] = {
        postMigrationExecuted = true
        postMigrationResult
      }
    }
  }
}
