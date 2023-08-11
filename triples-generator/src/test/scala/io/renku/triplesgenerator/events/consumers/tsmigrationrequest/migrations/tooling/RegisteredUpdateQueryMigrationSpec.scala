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
package migrations.tooling

import Generators.migrationNames
import cats.MonadThrow
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesgenerator.generators.ErrorGenerators.processingRecoverableErrors
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.typelevel.log4cats.Logger

class RegisteredUpdateQueryMigrationSpec
    extends AsyncWordSpec
    with CustomAsyncIOSpec
    with AsyncMockFactory
    with should.Matchers
    with EitherValues {

  "RegisteredUpdateQueryMigration" should {

    "be the RegisteredMigration" in {
      migration.getClass.getSuperclass shouldBe classOf[RegisteredMigration[IO]]
    }
  }

  "migrate" should {

    "execute the defined query" in {

      (queryRunner.run _).expects(query).returning(().pure[IO])

      migration.migrate().value.asserting(_.value shouldBe ())
    }

    "return a Recoverable Error if in case of an exception the given strategy returns one" in {

      val exception = exceptions.generateOne
      (queryRunner.run _)
        .expects(query)
        .returning(exception.raiseError[IO, Unit])

      migration.migrate().value.asserting(_.left.value shouldBe recoverableError)
    }
  }

  private lazy val query             = sparqlQueries.generateOne
  private lazy val queryRunner       = mock[UpdateQueryRunner[IO]]
  private lazy val executionRegister = mock[MigrationExecutionRegister[IO]]
  private lazy val recoverableError  = processingRecoverableErrors.generateOne
  private lazy val recoveryStrategy = new RecoverableErrorsRecovery {
    override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
      recoverableError.asLeft[OUT].pure[F]
    }
  }
  private implicit lazy val logger: Logger[IO] = TestLogger[IO]()
  private lazy val migration = new RegisteredUpdateQueryMigration[IO](migrationNames.generateOne,
                                                                      query,
                                                                      executionRegister,
                                                                      queryRunner,
                                                                      recoveryStrategy
  )
}
