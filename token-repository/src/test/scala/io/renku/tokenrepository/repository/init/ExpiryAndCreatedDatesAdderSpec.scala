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

package io.renku.tokenrepository.repository.init

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.interpreters.TestLogger.Level.Info
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class ExpiryAndCreatedDatesAdderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with DbInitSpec
    with should.Matchers
    with AsyncMockFactory {

  protected override lazy val runMigrationsUpTo: Class[_ <: DBMigration[IO]] =
    classOf[ExpiryAndCreatedDatesAdder[IO]]

  it should "create 'expiry_date' and 'created_at' columns; do nothing if they already exist" in testDBResource.use {
    implicit cfg =>
      for {
        _ <- verifyColumnExists("projects_tokens", "expiry_date").asserting(_ shouldBe false)
        _ <- verifyColumnExists("projects_tokens", "created_at").asserting(_ shouldBe false)

        _ <- logger.resetF()
        _ <- ExpiryAndCreatedDatesAdder[IO].run.assertNoException
        _ <- verifyColumnExists("projects_tokens", "expiry_date").asserting(_ shouldBe true)
        _ <- verifyColumnExists("projects_tokens", "created_at").asserting(_ shouldBe true)
        _ <- verifyIndexExists("projects_tokens", "idx_expiry_date").asserting(_ shouldBe true)
        _ <- verifyIndexExists("projects_tokens", "idx_created_at").asserting(_ shouldBe true)
        _ <- logger.loggedOnlyF(Info("'expiry_date' column added"), Info("'created_at' column added"))

        _ <- logger.resetF()
        _ <- ExpiryAndCreatedDatesAdder[IO].run.assertNoException
        _ <- logger.loggedOnlyF(Info("'expiry_date' column existed"), Info("'created_at' column existed"))
      } yield Succeeded
  }
}
