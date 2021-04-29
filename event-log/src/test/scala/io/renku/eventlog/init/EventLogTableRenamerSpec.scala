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

import cats.data.Kleisli
import cats.effect.IO
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.implicits._

class EventLogTableRenamerSpec extends AnyWordSpec with DbInitSpec with should.Matchers {

  protected override lazy val migrationsToRun: List[Migration] = List(
    eventLogTableCreator,
    projectPathAdder,
    batchDateAdder,
    projectTableCreator,
    projectPathRemover
  )

  "run" should {

    "rename the 'event_log' table to 'event' when 'event' does not exist" in new TestCase {

      tableExists("event_log") shouldBe true

      tableRenamer.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("event_log") shouldBe false
      tableExists("event")     shouldBe true

      logger.loggedOnly(Info("'event_log' table renamed to 'event'"))
    }

    "do nothing if the 'event' table already exists and 'event_log' does not exist" in new TestCase {

      tableExists("event_log") shouldBe true

      tableRenamer.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("event_log") shouldBe false
      tableExists("event")     shouldBe true

      logger.loggedOnly(Info("'event_log' table renamed to 'event'"))

      logger.reset()

      tableRenamer.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("'event' table already exists"))
    }

    "drop 'event_log' table if both the 'event' and 'event_log' tables exist" in new TestCase {

      tableExists("event_log") shouldBe true

      tableRenamer.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("event_log") shouldBe false
      tableExists("event")     shouldBe true

      logger.loggedOnly(Info("'event_log' table renamed to 'event'"))

      logger.reset()

      createEventLogTable() shouldBe ((): Unit)

      tableExists("event_log") shouldBe true
      tableExists("event")     shouldBe true

      tableRenamer.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("event_log") shouldBe false
      tableExists("event")     shouldBe true

      logger.loggedOnly(Info("'event_log' table dropped"))
    }
  }

  private trait TestCase {
    val logger       = TestLogger[IO]()
    val tableRenamer = new EventLogTableRenamerImpl[IO](sessionResource, logger)
  }

  private def createEventLogTable(): Unit = execute[Unit] {
    Kleisli { session =>
      val query: Command[Void] = sql"""
    CREATE TABLE IF NOT EXISTS event_log(
      event_id       varchar   NOT NULL,
      project_id     int4      NOT NULL,
      status         varchar   NOT NULL,
      created_date   timestamp NOT NULL,
      execution_date timestamp NOT NULL,
      event_date     timestamp NOT NULL,
      event_body     text      NOT NULL,
      message        varchar,
      PRIMARY KEY (event_id, project_id)
    );
    """.command
      session
        .execute(query)
        .map(_ => ())
    }
  }
}
