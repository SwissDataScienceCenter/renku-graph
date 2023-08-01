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
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectSlugAdderSpec
    extends AnyWordSpec
    with IOSpec
    with DbInitSpec
    with Eventually
    with IntegrationPatience
    with should.Matchers
    with MockFactory {

  protected override lazy val migrationsToRun: List[DBMigration[IO]] = allMigrations.takeWhile {
    case _: ProjectPathAdder[IO] => false
    case _ => true
  }

  "run" should {

    "add the 'project_path' column if does not exist and add paths fetched from GitLab" in new TestCase {

      verifyColumnExists("projects_tokens", "project_path") shouldBe false

      projectPathAdder.run.unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info("'project_path' column added"))
      logger.reset()

      projectPathAdder.run.unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info("'project_path' column existed"))

      verifyIndexExists("projects_tokens", "idx_project_path")
    }
  }

  private trait TestCase {
    logger.reset()

    val projectPathAdder = new ProjectPathAdder[IO]
  }
}
