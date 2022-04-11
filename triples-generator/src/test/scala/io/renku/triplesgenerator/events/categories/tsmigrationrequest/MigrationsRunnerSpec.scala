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

import cats.data.EitherT.{leftT, liftF, rightT}
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.generators.ErrorGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class MigrationsRunnerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "run" should {

    "succeed if all migrations run successfully" in new TestCase {
      (migration1.run _).expects().returning(rightT[Try, ProcessingRecoverableError](()))
      (migration2.run _).expects().returning(rightT[Try, ProcessingRecoverableError](()))
      (migration3.run _).expects().returning(rightT[Try, ProcessingRecoverableError](()))

      runner.run().value shouldBe ().asRight.pure[Try]

      logger.loggedOnly(
        Info(s"$categoryName: migration1 starting"),
        Info(s"$categoryName: migration1 done"),
        Info(s"$categoryName: migration2 starting"),
        Info(s"$categoryName: migration2 done"),
        Info(s"$categoryName: migration3 starting"),
        Info(s"$categoryName: migration3 done")
      )
    }

    "log an error and stop executing next migrations " +
      "once one of the migrations returns a Recoverable Error" in new TestCase {
        (migration1.run _).expects().returning(rightT[Try, ProcessingRecoverableError](()))

        val recoverableError = processingRecoverableErrors.generateOne
        (migration2.run _).expects().returning(leftT[Try, Unit](recoverableError))

        runner.run().value shouldBe recoverableError.asLeft.pure[Try]

        logger.loggedOnly(
          Info(s"$categoryName: migration1 starting"),
          Info(s"$categoryName: migration1 done"),
          Info(s"$categoryName: migration2 starting"),
          Error(s"$categoryName: migration2 failed: ${recoverableError.message}", recoverableError.cause)
        )
      }

    "log an error and fail if once one of the migration fails" in new TestCase {
      (migration1.run _).expects().returning(rightT[Try, ProcessingRecoverableError](()))

      val exception = exceptions.generateOne
      (migration2.run _)
        .expects()
        .returning(liftF(exception.raiseError[Try, Unit]))

      runner.run().value shouldBe exception.raiseError[Try, Either[ProcessingRecoverableError, Unit]]

      logger.loggedOnly(
        Info(s"$categoryName: migration1 starting"),
        Info(s"$categoryName: migration1 done"),
        Info(s"$categoryName: migration2 starting"),
        Error(s"$categoryName: migration2 failed", exception)
      )
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[Try] = TestLogger[Try]()

    val migration1 = mock[Migration[Try]]
    (() => migration1.name).expects().returning(Migration.Name("migration1")).anyNumberOfTimes()

    val migration2 = mock[Migration[Try]]
    (() => migration2.name).expects().returning(Migration.Name("migration2")).anyNumberOfTimes()

    val migration3 = mock[Migration[Try]]
    (() => migration3.name).expects().returning(Migration.Name("migration3")).anyNumberOfTimes()

    val runner = new MigrationsRunnerImpl[Try](List(migration1, migration2, migration3))
  }
}
