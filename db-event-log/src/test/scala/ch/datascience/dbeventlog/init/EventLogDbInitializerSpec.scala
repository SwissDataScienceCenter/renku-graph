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
import cats.implicits._
import ch.datascience.dbeventlog.InMemoryEventLogDb
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import doobie.implicits._
import doobie.util.fragment.Fragment
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogDbInitializerSpec extends WordSpec with InMemoryEventLogDb {

  "run" should {

    "do nothing if event_log table already exists" in new TestCase {
      if (!tableExists()) createTable()

      tableExists() shouldBe true

      dbInitializer.run.unsafeRunSync() shouldBe ()

      tableExists() shouldBe true

      logger.loggedOnly(Info("Event Log database initialization success"))
    }

    "create the event_log table if it does not exist" in new TestCase {
      if (tableExists()) dropTable()

      tableExists() shouldBe false

      dbInitializer.run.unsafeRunSync() shouldBe ()

      tableExists() shouldBe true

      logger.loggedOnly(Info("Event Log database initialization success"))
    }

    "assures there are indexes created for project_id, status and execution_date" in new TestCase {

      dbInitializer.run.unsafeRunSync() shouldBe ()

      verifyTrue(sql"DROP INDEX idx_project_id;")
      verifyTrue(sql"DROP INDEX idx_status;")
      verifyTrue(sql"DROP INDEX idx_execution_date;")
      verifyTrue(sql"DROP INDEX idx_event_date;")
    }
  }

  private trait TestCase {
    val logger        = TestLogger[IO]()
    val dbInitializer = new EventLogDbInitializer[IO](transactor, logger)
  }

  private def tableExists(): Boolean =
    sql"""select exists (select * from event_log);""".query.option
      .transact(transactor.get)
      .recover { case _ => None }
      .unsafeRunSync()
      .isDefined

  private def createTable(): Unit = execute {
    sql"""
         |CREATE TABLE event_log(
         | event_id varchar NOT NULL,
         | project_id int4 NOT NULL,
         | status varchar NOT NULL,
         | created_date timestamp NOT NULL,
         | execution_date timestamp NOT NULL,
         | event_date timestamp NOT NULL,
         | event_body text NOT NULL,
         | message varchar,
         | PRIMARY KEY (event_id, project_id)
         |);
       """.stripMargin.update.run
  }

  protected def dropTable(): Unit = execute {
    sql"DROP TABLE IF EXISTS event_log".update.run
  }

  protected def verifyTrue(sql: Fragment): Unit = execute {
    sql.update.run
  }
}
