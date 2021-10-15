/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import cats.effect._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.testtools.MockedRunnableCollaborators
import io.renku.tokenrepository.repository.InMemoryProjectsTokensDb
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DbInitializerSpec
    extends AnyWordSpec
    with InMemoryProjectsTokensDb
    with MockFactory
    with should.Matchers
    with MockedRunnableCollaborators {

  "run" should {

    "do nothing if projects_tokens table already exists" in new TestCase {
      if (!tableExists()) createTable()

      tableExists() shouldBe true

      given(projectPathAdder).succeeds(returning = ())
      given(duplicateProjectsRemover).succeeds(returning = ())

      dbInitializer.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists() shouldBe true

      logger.loggedOnly(Info("Projects Tokens database initialization success"))
    }

    "create the projects_tokens table if id does not exist" in new TestCase {
      if (tableExists()) dropTable()

      tableExists() shouldBe false

      given(projectPathAdder).succeeds(returning = ())
      given(duplicateProjectsRemover).succeeds(returning = ())

      dbInitializer.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists() shouldBe true

      logger.loggedOnly(Info("Projects Tokens database initialization success"))
    }

    "fail if the Projects Paths adding process fails" in new TestCase {
      if (tableExists()) dropTable()

      tableExists() shouldBe false

      val exception = exceptions.generateOne
      given(projectPathAdder).fails(becauseOf = exception)

      intercept[Exception] {
        dbInitializer.run().unsafeRunSync() shouldBe ((): Unit)
      } shouldBe exception

      logger.loggedOnly(Error("Projects Tokens database initialization failure", exception))
    }

    "fail if the Projects de-duplication fails" in new TestCase {
      if (tableExists()) dropTable()

      tableExists() shouldBe false

      given(projectPathAdder).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(duplicateProjectsRemover).fails(becauseOf = exception)

      intercept[Exception] {
        dbInitializer.run().unsafeRunSync() shouldBe ((): Unit)
      } shouldBe exception

      logger.loggedOnly(Error("Projects Tokens database initialization failure", exception))
    }
  }

  private trait TestCase {
    val logger                   = TestLogger[IO]()
    val projectPathAdder         = mock[ProjectPathAdder[IO]]
    val duplicateProjectsRemover = mock[DuplicateProjectsRemover[IO]]
    val dbInitializer            = new DbInitializer[IO](projectPathAdder, duplicateProjectsRemover, sessionResource, logger)
  }
}
