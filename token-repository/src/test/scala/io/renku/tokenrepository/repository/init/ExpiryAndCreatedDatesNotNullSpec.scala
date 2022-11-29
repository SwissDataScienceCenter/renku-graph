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

package io.renku.tokenrepository.repository.init

import cats.effect.IO
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ExpiryAndCreatedDatesNotNullSpec
    extends AnyWordSpec
    with IOSpec
    with DbInitSpec
    with should.Matchers
    with MockFactory {

  protected override lazy val migrationsToRun: List[DBMigration[IO]] = allMigrations.takeWhile {
    case _: ExpiryAndCreatedDatesNotNull[IO] => false
    case _ => true
  }

  "run" should {

    "make the expiry_date and created_at NOT NULL" in new TestCase {

      verifyColumnNullable("projects_tokens", "expiry_date") shouldBe true
      verifyColumnNullable("projects_tokens", "created_at")  shouldBe true

      migration.run().unsafeRunSync() shouldBe ()

      verifyColumnNullable("projects_tokens", "expiry_date") shouldBe false
      verifyColumnNullable("projects_tokens", "created_at")  shouldBe false

      logger.loggedOnly(Info("'expiry_date' column made NOT NULL"), Info("'created_at' column made NOT NULL"))

      logger.reset()

      migration.run().unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info("'expiry_date' column already NOT NULL"), Info("'created_at' column already NOT NULL"))
    }
  }

  private trait TestCase {
    logger.reset()

    val migration = new ExpiryAndCreatedDatesNotNull[IO]
  }
}
