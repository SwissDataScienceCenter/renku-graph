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

package io.renku.eventlog.init

import cats.effect.IO
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ProjectPathToSlugSpec
    extends AnyFlatSpec
    with should.Matchers
    with IOSpec
    with DbInitSpec
    with Eventually
    with IntegrationPatience {

  protected[init] override lazy val migrationsToRun: List[DbMigrator[IO]] = allMigrations.takeWhile {
    case _: ProjectPathToSlugImpl[IO] => false
    case _ => true
  }

  it should "rename the 'project_path' column to 'project_slug' on the project and clean_up_events_queue tables, " +
    "remove the old 'idx_project_path' index and " +
    "create new 'idx_project_project_slug' and 'idx_clean_up_events_queue_project_slug' indices" in {

      verifyColumnExists("project", "project_path")                                        shouldBe true
      verifyColumnExists("project", "project_slug")                                        shouldBe false
      verifyColumnExists("clean_up_events_queue", "project_path")                          shouldBe true
      verifyColumnExists("clean_up_events_queue", "project_slug")                          shouldBe false
      verifyIndexExists("project", "idx_project_path")                                     shouldBe false
      verifyIndexExists("project", "idx_project_project_slug")                             shouldBe false
      verifyIndexExists("clean_up_events_queue", "idx_project_path")                       shouldBe true
      verifyIndexExists("clean_up_events_queue", "idx_clean_up_events_queue_project_slug") shouldBe false

      projectPathToSlug.run.unsafeRunSync()

      logger.loggedOnly(
        Info("column 'project.project_path' renamed to 'project.project_slug'"),
        Info("column 'clean_up_events_queue.project_path' renamed to 'clean_up_events_queue.project_slug'"),
        Info("index 'idx_project_path' removed"),
        Info("index 'idx_project_project_slug' created"),
        Info("index 'idx_clean_up_events_queue_project_slug' created")
      )
      logger.reset()

      verifyColumnExists("project", "project_path")                                        shouldBe false
      verifyColumnExists("project", "project_slug")                                        shouldBe true
      verifyColumnExists("clean_up_events_queue", "project_path")                          shouldBe false
      verifyColumnExists("clean_up_events_queue", "project_slug")                          shouldBe true
      verifyIndexExists("project", "idx_project_path")                                     shouldBe false
      verifyIndexExists("project", "idx_project_project_slug")                             shouldBe true
      verifyIndexExists("clean_up_events_queue", "idx_project_path")                       shouldBe false
      verifyIndexExists("clean_up_events_queue", "idx_clean_up_events_queue_project_slug") shouldBe true

      projectPathToSlug.run.unsafeRunSync()

      logger.loggedOnly(
        Info("column 'project.project_slug' existed"),
        Info("column 'clean_up_events_queue.project_slug' existed"),
        Info("index 'idx_project_path' not existed"),
        Info("index 'idx_project_project_slug' existed"),
        Info("index 'idx_clean_up_events_queue_project_slug' existed")
      )
    }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private lazy val projectPathToSlug = new ProjectPathToSlugImpl[IO]
}
