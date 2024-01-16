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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest

import cats.data.EitherT
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.config.ServiceVersion
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import migrations.tooling.MigrationExecutionRegister
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class MigrationStatusCheckerSpec extends AsyncFlatSpec with AsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "return true if the last exclusive Migration has not been executed yet" in {

    val lastMigration = exclusiveMigration(names.generateOne)

    givenMigration(lastMigration, wasExecuted = false)

    val checker = newStatusChecker(
      nonExclusiveMigration(names.generateOne),
      exclusiveMigration(names.generateOne),
      lastMigration,
      nonExclusiveMigration(names.generateOne)
    )

    checker.underMigration.asserting(_ shouldBe true)
  }

  it should "return false if there's no exclusive Migration on the list of migrations" in {

    val checker = newStatusChecker(
      nonExclusiveMigration(names.generateOne),
      nonExclusiveMigration(names.generateOne)
    )

    checker.underMigration.asserting(_ shouldBe false)
  }

  it should "return false if the last exclusive Migration was executed" in {

    val lastMigration = exclusiveMigration(names.generateOne)

    givenMigration(lastMigration, wasExecuted = true)

    val checker = newStatusChecker(
      nonExclusiveMigration(names.generateOne),
      exclusiveMigration(names.generateOne),
      lastMigration,
      nonExclusiveMigration(names.generateOne)
    )

    checker.underMigration.asserting(_ shouldBe false)
  }

  private def exclusiveMigration(n: Migration.Name) =
    newMigration(n, exclsv = true)
  private def nonExclusiveMigration(n: Migration.Name) =
    newMigration(n, exclsv = false)

  private def newMigration(n: Migration.Name, exclsv: Boolean) =
    new Migration[IO] {
      override val name:      Migration.Name                                = n
      override def exclusive: Boolean                                       = exclsv
      override def run():     EitherT[IO, ProcessingRecoverableError, Unit] = EitherT.pure(())
    }

  private val executionRegister = mock[MigrationExecutionRegister[IO]]
  private def newStatusChecker(migrations: Migration[IO]*) =
    new MigrationStatusCheckerImpl[IO](executionRegister, migrations.toList)

  private def givenMigration(migration: Migration[IO], wasExecuted: Boolean) =
    (executionRegister.findExecution _)
      .expects(migration.name)
      .returning {
        if (wasExecuted) serviceVersions.generateSome.pure[IO]
        else Option.empty[ServiceVersion].pure[IO]
      }

  private lazy val names: Gen[Migration.Name] = nonEmptyStrings().generateAs(Migration.Name(_))
}
