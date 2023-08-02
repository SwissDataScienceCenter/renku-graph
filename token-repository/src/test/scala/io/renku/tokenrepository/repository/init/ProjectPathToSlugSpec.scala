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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ProjectPathToSlugSpec
    extends AnyFlatSpec
    with IOSpec
    with DbInitSpec
    with Eventually
    with IntegrationPatience
    with should.Matchers
    with MockFactory {

  protected override lazy val migrationsToRun: List[DBMigration[IO]] = allMigrations.takeWhile {
    case _: ProjectPathToSlug[IO] => false
    case _ => true
  }

  it should "rename the 'project_path' column to 'project_slug' " +
    "as well as rename the 'idx_project_path' index" in {

      verifyColumnExists("projects_tokens", "project_path") shouldBe true
      verifyColumnExists("projects_tokens", "project_slug") shouldBe false

      projectPathToSlug.run.unsafeRunSync()

      logger.loggedOnly(Info("column 'project_path' renamed to 'project_slug'"))
      logger.reset()

      verifyColumnExists("projects_tokens", "project_path") shouldBe false
      verifyColumnExists("projects_tokens", "project_slug") shouldBe true

      projectPathToSlug.run.unsafeRunSync()

      logger.loggedOnly(Info("'project_slug' column existed"))

      verifyIndexExists("projects_tokens", "idx_project_path") shouldBe false
      verifyIndexExists("projects_tokens", "idx_project_slug") shouldBe true
    }

  private lazy val projectPathToSlug = new ProjectPathToSlug[IO]
}
