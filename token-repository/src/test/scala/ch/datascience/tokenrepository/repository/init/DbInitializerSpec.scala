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

import cats.MonadError
import cats.effect._
import cats.implicits._
import ch.datascience.db.DBConfigProvider.DBConfig
import ch.datascience.db.{DbSpec, TransactorProvider}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.orchestration.Provider
import doobie.implicits._
import doobie.util.transactor.Transactor.Aux
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class DbInitializerSpec extends WordSpec with DbSpec with MockFactory {

  "run" should {

    "do nothing if projects_tokens table already exists" in new TestCase {
      if (!tableExists()) createTable()

      tableExists() shouldBe true

      dbInitializer.run.unsafeRunSync() shouldBe ExitCode.Success

      tableExists() shouldBe true

      logger.loggedOnly(Info("Database initialization success"))
    }

    "create the projects_tokens table if id does not exist" in new TestCase {
      if (tableExists()) dropTable()

      tableExists() shouldBe false

      dbInitializer.run.unsafeRunSync() shouldBe ExitCode.Success

      tableExists() shouldBe true

      logger.loggedOnly(Info("Database initialization success"))
    }

    "fail if table creation process fails" in new MockTestCase {
      val exception = exceptions.generateOne
      (dbConfigProvider.get _)
        .expects()
        .returning(context.raiseError(exception))

      intercept[Exception] {
        dbInitializer.run.unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(Error("Database initialization failure", exception))
    }
  }

  private trait TestCase {
    val logger        = TestLogger[IO]()
    val dbInitializer = new DbInitializer[IO](transactorProvider, logger)
  }

  private trait MockTestCase {
    val context = MonadError[IO, Throwable]

    val logger           = TestLogger[IO]()
    val dbConfigProvider = mock[Provider[IO, DBConfig]]
    val dbInitializer = new DbInitializer[IO](
      new TransactorProvider[IO](dbConfigProvider),
      logger
    )
  }

  private def tableExists(): Boolean =
    sql"""select exists (select * from projects_tokens);""".query.option
      .transact(transactor)
      .recover {
        case _ => None
      }
      .unsafeRunSync()
      .isDefined

  private def createTable(): Unit =
    sql"""
         |CREATE TABLE projects_tokens(
         | project_id int4 PRIMARY KEY,
         | token VARCHAR (100) NOT NULL
         |);
       """.stripMargin.update.run
      .transact(transactor)
      .unsafeRunSync()

  protected def dropTable(): Unit =
    sql"DROP TABLE IF EXISTS projects_tokens".update.run
      .transact(transactor)
      .unsafeRunSync()

  protected override def initDb(transactor:           Aux[IO, Unit]): Unit = ()
  protected override def prepareDbForTest(transactor: Aux[IO, Unit]): Unit = ()
}
