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
package migrations.tooling

import cats.MonadThrow
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.triplesgenerator.events.categories.tsmigrationrequest.Generators.migrationNames
import io.renku.triplesgenerator.generators.ErrorGenerators.processingRecoverableErrors
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class UpdateQueryMigrationSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {

    "execute the defined query" in new TestCase {

      (queryRunner.run _).expects(query).returning(().pure[Try])

      migration.run().value shouldBe ().asRight.pure[Try]

      logger.loggedOnly(Info(show"$categoryName: ${migration.name} starting"),
                        Info(show"$categoryName: ${migration.name} done")
      )
    }

    "return a Recoverable Error if in case of an exception the given strategy returns one" in new TestCase {
      val exception = exceptions.generateOne
      (queryRunner.run _)
        .expects(query)
        .returning(exception.raiseError[Try, Unit])

      migration.run().value shouldBe recoverableError.asLeft.pure[Try]
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val query            = sparqlQueries.generateOne
    val queryRunner      = mock[UpdateQueryRunner[Try]]
    val recoverableError = processingRecoverableErrors.generateOne
    val recoveryStrategy = new RecoverableErrorsRecovery {
      override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
        recoverableError.asLeft[OUT].pure[F]
      }
    }
    val migration = new UpdateQueryMigration[Try](migrationNames.generateOne, query, queryRunner, recoveryStrategy)
  }
}
