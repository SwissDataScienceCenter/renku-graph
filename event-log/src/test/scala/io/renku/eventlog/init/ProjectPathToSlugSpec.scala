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

package io.renku.eventlog.init

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.EventLogDB
import io.renku.interpreters.TestLogger.Level.Info
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class ProjectPathToSlugSpec extends AsyncFlatSpec with AsyncIOSpec with DbInitSpec with should.Matchers {

  protected[init] override val runMigrationsUpTo: Class[_ <: DbMigrator[IO]] = classOf[ProjectPathToSlug[IO]]

  it should "rename the 'project_path' column to 'project_slug' on the project and clean_up_events_queue tables, " +
    "remove the old 'idx_project_path' index and " +
    "create new 'idx_project_project_slug' and 'idx_clean_up_events_queue_project_slug' indices" in testDBResource.use {
      implicit cfg =>
        for {
          _ <- verifyColumnExists("project", "project_path").asserting(_ shouldBe true)
          _ <- verifyColumnExists("project", "project_slug").asserting(_ shouldBe false)
          _ <- verifyColumnExists("clean_up_events_queue", "project_path").asserting(_ shouldBe true)
          _ <- verifyColumnExists("clean_up_events_queue", "project_slug").asserting(_ shouldBe false)
          _ <- verifyIndexExists("project", "idx_project_path").asserting(_ shouldBe false)
          _ <- verifyIndexExists("project", "idx_project_project_slug").asserting(_ shouldBe false)
          _ <- verifyIndexExists("clean_up_events_queue", "idx_project_path").asserting(_ shouldBe true)
          _ <- verifyIndexExists("clean_up_events_queue", "idx_clean_up_events_queue_project_slug")
                 .asserting(_ shouldBe false)

          _ <- projectPathToSlug.run.assertNoException

          _ <- logger.loggedOnlyF(
                 Info("column 'project.project_path' renamed to 'project.project_slug'"),
                 Info("column 'clean_up_events_queue.project_path' renamed to 'clean_up_events_queue.project_slug'"),
                 Info("index 'idx_project_path' removed"),
                 Info("index 'idx_project_project_slug' created"),
                 Info("index 'idx_clean_up_events_queue_project_slug' created")
               )
          _ <- logger.resetF()

          _ <- verifyColumnExists("project", "project_path").asserting(_ shouldBe false)
          _ <- verifyColumnExists("project", "project_slug").asserting(_ shouldBe true)
          _ <- verifyColumnExists("clean_up_events_queue", "project_path").asserting(_ shouldBe false)
          _ <- verifyColumnExists("clean_up_events_queue", "project_slug").asserting(_ shouldBe true)
          _ <- verifyIndexExists("project", "idx_project_path").asserting(_ shouldBe false)
          _ <- verifyIndexExists("project", "idx_project_project_slug").asserting(_ shouldBe true)
          _ <- verifyIndexExists("clean_up_events_queue", "idx_project_path").asserting(_ shouldBe false)
          _ <- verifyIndexExists("clean_up_events_queue", "idx_clean_up_events_queue_project_slug")
                 .asserting(_ shouldBe true)

          _ <- projectPathToSlug.run.assertNoException

          _ <- logger.loggedOnlyF(
                 Info("column 'project.project_slug' existed"),
                 Info("column 'clean_up_events_queue.project_slug' existed"),
                 Info("index 'idx_project_path' not existed"),
                 Info("index 'idx_project_project_slug' existed"),
                 Info("index 'idx_clean_up_events_queue_project_slug' existed")
               )
        } yield Succeeded
    }

  private def projectPathToSlug(implicit cfg: DBConfig[EventLogDB]) = new ProjectPathToSlugImpl[IO]
}
