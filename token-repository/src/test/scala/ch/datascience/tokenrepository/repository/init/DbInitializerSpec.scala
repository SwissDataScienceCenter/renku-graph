/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.tokenrepository.repository.init

import cats.effect._
import cats.implicits._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import ch.datascience.tokenrepository.repository.InMemoryProjectsTokensDb
import doobie.implicits._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class DbInitializerSpec extends WordSpec with InMemoryProjectsTokensDb {

  "run" should {

    "do nothing if projects_tokens table already exists" in new TestCase {
      if (!tableExists()) createTable()

      tableExists() shouldBe true

      dbInitializer.run.unsafeRunSync() shouldBe ExitCode.Success

      tableExists() shouldBe true

      logger.loggedOnly(Info("Projects Tokens database initialization success"))
    }

    "create the projects_tokens table if id does not exist" in new TestCase {
      if (tableExists()) dropTable()

      tableExists() shouldBe false

      dbInitializer.run.unsafeRunSync() shouldBe ExitCode.Success

      tableExists() shouldBe true

      logger.loggedOnly(Info("Projects Tokens database initialization success"))
    }
  }

  private trait TestCase {
    val logger        = TestLogger[IO]()
    val dbInitializer = new DbInitializer[IO](transactor, logger)
  }

  private def tableExists(): Boolean =
    sql"""select exists (select * from projects_tokens);""".query.option
      .transact(transactor.get)
      .recover { case _ => None }
      .unsafeRunSync()
      .isDefined

  private def createTable(): Unit = execute {
    sql"""
         |CREATE TABLE projects_tokens(
         | project_id int4 PRIMARY KEY,
         | token VARCHAR NOT NULL
         |);
       """.stripMargin.update.run.map(_ => ())
  }

  protected def dropTable(): Unit = execute {
    sql"DROP TABLE IF EXISTS projects_tokens".update.run.map(_ => ())
  }
}
