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

package ch.datascience.dbeventlog.init

import cats.effect._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import doobie.implicits._
import io.chrisdavenport.log4cats.Logger
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogDbInitializerSpec extends WordSpec with DbInitSpec with MockFactory {

  "run" should {

    "do nothing if event_log table already exists" in new TestCase {
      if (!tableExists()) createTable()

      tableExists() shouldBe true

      (projectPathAdder.run _)
        .expects()
        .returning(IO.unit)

      dbInitializer.run.unsafeRunSync() shouldBe ((): Unit)

      tableExists() shouldBe true

      logger.loggedOnly(Info("Event Log database initialization success"))
    }

    "create the event_log table if it does not exist" in new TestCase {
      if (tableExists()) dropTable()

      tableExists() shouldBe false

      (projectPathAdder.run _)
        .expects()
        .returning(IO.unit)

      dbInitializer.run.unsafeRunSync() shouldBe ((): Unit)

      tableExists() shouldBe true

      logger.loggedOnly(Info("Event Log database initialization success"))
    }

    "assures there are indexes created for project_id, status and execution_date" in new TestCase {

      if (!tableExists()) createTable()

      tableExists() shouldBe true

      (projectPathAdder.run _)
        .expects()
        .returning(IO.unit)

      dbInitializer.run.unsafeRunSync() shouldBe ((): Unit)

      verifyTrue(sql"DROP INDEX idx_project_id;")
      verifyTrue(sql"DROP INDEX idx_status;")
      verifyTrue(sql"DROP INDEX idx_execution_date;")
      verifyTrue(sql"DROP INDEX idx_event_date;")
      verifyTrue(sql"DROP INDEX idx_created_date;")
    }

    "fails if adding the project_path column fails" in new TestCase {

      if (!tableExists()) createTable()

      tableExists() shouldBe true

      val exception = exceptions.generateOne
      (projectPathAdder.run _)
        .expects()
        .returning(IO.raiseError(exception))

      intercept[Exception] {
        dbInitializer.run.unsafeRunSync()
      } shouldBe exception
    }
  }

  private trait TestCase {
    val projectPathAdder = mock[IOProjectPathAdder]
    val logger           = TestLogger[IO]()
    val dbInitializer    = new EventLogDbInitializer[IO](projectPathAdder, transactor, logger)
  }

  private class IOProjectPathAdder(transactor: DbTransactor[IO, EventLogDB], logger: Logger[IO])
      extends ProjectPathAdder[IO](transactor, logger)
}
