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

class ProjectPathRemoverSpec extends AsyncFlatSpec with AsyncIOSpec with DbInitSpec with should.Matchers {

  protected[init] override val runMigrationsUpTo: Class[_ <: DbMigrator[IO]] = classOf[ProjectPathRemover[IO]]

  it should "do nothing if the 'event' table already exists" in testDBResource.use { implicit cfg =>
    for {
      _ <- createEventTable >> logger.resetF()

      _ <- projectPathRemover.run.assertNoException

      _ <- logger.loggedOnlyF(Info("'project_path' column dropping skipped"))
    } yield Succeeded
  }

  it should "remove the 'project_path' column if it exists on the 'event_log' table" in testDBResource.use {
    implicit cfg =>
      for {
        _ <- verifyColumnExists("event_log", "project_path").asserting(_ shouldBe true)

        _ <- projectPathRemover.run.assertNoException

        _ <- verifyColumnExists("event_log", "project_path").asserting(_ shouldBe false)

        _ <- logger.loggedOnlyF(Info("'project_path' column removed"))

        _ <- logger.resetF()

        _ <- projectPathRemover.run.assertNoException

        _ <- logger.loggedOnlyF(Info("'project_path' column already removed"))
      } yield Succeeded
  }

  private def projectPathRemover(implicit cfg: DBConfig[EventLogDB]) = new ProjectPathRemoverImpl[IO]
}
