/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.MonadThrow
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.errors.ErrorGenerators.processingRecoverableErrors
import io.renku.triplesgenerator.generators.VersionGenerators.renkuVersionPairs
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import reprovisioning.RenkuVersionPairUpdater
import tooling.{MigrationExecutionRegister, RecoverableErrorsRecovery}

class V10VersionUpdaterSpec extends AnyWordSpec with should.Matchers with MockFactory with IOSpec with EitherValues {

  "migrate" should {

    "replace the current schema with v10 and cli version 2.3.0" in new TestCase {

      (renkuVersionPairUpdater.update _)
        .expects(renkuVersionPair)
        .returning(().pure[IO])

      setter.migrate().value.unsafeRunSync().value shouldBe ()
    }
  }

  private trait TestCase {

    private implicit val logger: TestLogger[IO] = TestLogger()
    val renkuVersionPair          = renkuVersionPairs.generateOne
    val renkuVersionPairUpdater   = mock[RenkuVersionPairUpdater[IO]]
    private val executionRegister = mock[MigrationExecutionRegister[IO]]
    private val recoverableError  = processingRecoverableErrors.generateOne
    private val recoveryStrategy = new RecoverableErrorsRecovery {
      override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
        recoverableError.asLeft[OUT].pure[F]
      }
    }
    val setter =
      new V10VersionUpdater[IO](renkuVersionPair, renkuVersionPairUpdater, executionRegister, recoveryStrategy)
  }
}
