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

import ConditionedMigration.MigrationRequired
import Generators._
import cats.data.EitherT
import cats.data.EitherT._
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import io.renku.triplesgenerator.generators.ErrorGenerators.processingRecoverableErrors
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.language.reflectiveCalls

class ConditionedMigrationSpec extends AnyWordSpec with IOSpec with should.Matchers {

  "run" should {

    "log the requirement checking result and do nothing if No" in new TestCase {
      val required  = migrationRequiredNo.generateOne
      val migration = createMigration(required)

      migration.run().value.unsafeRunSync() shouldBe ().asRight

      migration.migrationExecuted     shouldBe false
      migration.postMigrationExecuted shouldBe false

      logger.loggedOnly(Info(show"$categoryName: ${migration.name} $required"))
    }

    "log the requirement checking result and run the migration if Yes" in new TestCase {
      val required  = migrationRequiredYes.generateOne
      val migration = createMigration(required)

      migration.run().value.unsafeRunSync() shouldBe ().asRight

      migration.migrationExecuted     shouldBe true
      migration.postMigrationExecuted shouldBe true

      logger.loggedOnly(Info(show"$categoryName: ${migration.name} $required"))
    }

    "return Processing Recoverable Failure if the migration process returns one" in new TestCase {
      val required         = migrationRequiredYes.generateOne
      val recoverableError = processingRecoverableErrors.generateOne
      val migrationResult  = leftT[IO, Unit](recoverableError)
      val migration        = createMigration(required, migrationResult)

      migration.run().value.unsafeRunSync() shouldBe Left(recoverableError)

      migration.migrationExecuted     shouldBe true
      migration.postMigrationExecuted shouldBe false

      logger.loggedOnly(Info(show"$categoryName: ${migration.name} $required"))
    }

    "fail if the migration process fails" in new TestCase {
      val required        = migrationRequiredYes.generateOne
      val exception       = exceptions.generateOne
      val migrationResult = right[ProcessingRecoverableError](exception.raiseError[IO, Unit])
      val migration       = createMigration(required, migrationResult)

      intercept[Exception](migration.run().value.unsafeRunSync()) shouldBe exception

      migration.migrationExecuted     shouldBe true
      migration.postMigrationExecuted shouldBe false

      logger.loggedOnly(Info(show"$categoryName: ${migration.name} $required"))
    }

    "return Processing Recoverable Failure if the post migration process returns one" in new TestCase {
      val required            = migrationRequiredYes.generateOne
      val recoverableError    = processingRecoverableErrors.generateOne
      val postMigrationResult = leftT[IO, Unit](recoverableError)
      val migration           = createMigration(required, postMigrationResult = postMigrationResult)

      migration.run().value.unsafeRunSync() shouldBe Left(recoverableError)

      migration.migrationExecuted     shouldBe true
      migration.postMigrationExecuted shouldBe true

      logger.loggedOnly(Info(show"$categoryName: ${migration.name} $required"))
    }

    "fail if the post migration process fails" in new TestCase {
      val required            = migrationRequiredYes.generateOne
      val exception           = exceptions.generateOne
      val postMigrationResult = right[ProcessingRecoverableError](exception.raiseError[IO, Unit])
      val migration           = createMigration(required, postMigrationResult = postMigrationResult)

      intercept[Exception](migration.run().value.unsafeRunSync()) shouldBe exception

      migration.migrationExecuted     shouldBe true
      migration.postMigrationExecuted shouldBe true

      logger.loggedOnly(Info(show"$categoryName: ${migration.name} $required"))
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()

    def createMigration(migrationRequired:   MigrationRequired,
                        migrationResult:     EitherT[IO, ProcessingRecoverableError, Unit] = rightT(()),
                        postMigrationResult: EitherT[IO, ProcessingRecoverableError, Unit] = rightT(())
    ) = new ConditionedMigration[IO] {
      override val name     = migrationNames.generateOne
      override val required = rightT(migrationRequired)

      var migrationExecuted: Boolean = false
      protected override def migrate(): EitherT[IO, ProcessingRecoverableError, Unit] = {
        migrationExecuted = true
        migrationResult
      }

      var postMigrationExecuted: Boolean = false
      protected override def postMigration(): EitherT[IO, ProcessingRecoverableError, Unit] = {
        postMigrationExecuted = true
        postMigrationResult
      }
    }
  }
}
