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
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ExpireAndCreatedDatesAdderSpec extends AnyWordSpec with IOSpec with DbInitSpec with should.Matchers {

  protected override lazy val migrationsToRun: List[DBMigration[IO]] = allMigrations.takeWhile {
    case _: ExpireAndCreatedDatesAdder[IO] => false
    case _ => true
  }

  "run" should {

    "create 'expiry_date' and 'created_at' columns; do nothing if they already exist" in new TestCase {
      logger.reset()

      verifyColumnExists("projects_tokens", "expiry_date") shouldBe false
      verifyColumnExists("projects_tokens", "created_at")  shouldBe false

      datesAdder.run().unsafeRunSync() shouldBe ()

      verifyColumnExists("projects_tokens", "expiry_date")    shouldBe true
      verifyColumnExists("projects_tokens", "created_at")     shouldBe true
      verifyIndexExists("projects_tokens", "idx_expiry_date") shouldBe true
      verifyIndexExists("projects_tokens", "idx_created_at")  shouldBe true

      logger.loggedOnly(Info("'expiry_date' column added"), Info("'created_at' column added"))
      logger.reset()

      datesAdder.run().unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info("'expiry_date' column existed"), Info("'created_at' column existed"))
    }
  }

  private trait TestCase {
    val datesAdder = new ExpireAndCreatedDatesAdder[IO]
  }
}
