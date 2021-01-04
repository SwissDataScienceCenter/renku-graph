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

package io.renku.eventlog.init

import cats.effect.IO
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import doobie.implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventLogTableCreatorSpec extends AnyWordSpec with DbInitSpec with should.Matchers {

  protected override val migrationsToRun: List[Migration] = Nil

  "run" should {

    "do nothing if the 'event' table already exists" in new TestCase {

      createEventTable()
      tableExists("event")     shouldBe true
      tableExists("event_log") shouldBe false

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("event_log") shouldBe false

      logger.loggedOnly(Info("'event_log' table creation skipped"))
    }

    "create the 'event_log' table if it doesn't exist" in new TestCase {

      tableExists("event_log") shouldBe false

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("event_log") shouldBe true

      logger.loggedOnly(Info("'event_log' table created"))
    }

    "do nothing if the 'event_log' table already exists" in new TestCase {

      tableExists("event_log") shouldBe false

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("event_log") shouldBe true

      logger.loggedOnly(Info("'event_log' table created"))

      logger.reset()

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("'event_log' table exists"))
    }

    "create indices for certain columns" in new TestCase {

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("event_log") shouldBe true

      verifyTrue(sql"DROP INDEX idx_project_id;")
      verifyTrue(sql"DROP INDEX idx_event_id;")
      verifyTrue(sql"DROP INDEX idx_status;")
      verifyTrue(sql"DROP INDEX idx_execution_date;")
      verifyTrue(sql"DROP INDEX idx_event_date;")
      verifyTrue(sql"DROP INDEX idx_created_date;")
    }
  }

  private trait TestCase {
    val logger       = TestLogger[IO]()
    val tableCreator = new EventLogTableCreatorImpl[IO](transactor, logger)
  }
}
