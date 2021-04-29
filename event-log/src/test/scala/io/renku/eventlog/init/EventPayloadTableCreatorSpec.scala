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
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk.implicits._

class EventPayloadTableCreatorSpec extends AnyWordSpec with DbInitSpec with should.Matchers {

  protected override lazy val migrationsToRun: List[Migration] = List(
    eventLogTableCreator,
    projectPathAdder,
    batchDateAdder,
    projectTableCreator,
    projectPathRemover,
    eventLogTableRenamer,
    eventStatusRenamer
  )

  "run" should {
    "fail if the 'event' table does not exist" in new TestCase {
      dropTable("event")
      tableExists("event")         shouldBe false
      tableExists("event_payload") shouldBe false

      intercept[Exception] {

        tableCreator.run().unsafeRunSync()
      }.getMessage shouldBe "Event table missing; creation of event_payload is not possible"
    }

    "create the 'event_payload' table if it doesn't exist" in new TestCase {

      tableExists("event_payload") shouldBe false

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("event_payload") shouldBe true

      logger.loggedOnly(Info("'event_payload' table created"))
    }

    "do nothing if the 'event_payload' table already exists" in new TestCase {

      tableExists("event_payload") shouldBe false

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("event_payload") shouldBe true

      logger.loggedOnly(Info("'event_payload' table created"))

      logger.reset()

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("'event_payload' table exists"))
    }

    "create indices for certain columns" in new TestCase {

      tableExists("event_payload") shouldBe false

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("event_payload") shouldBe true

      verifyTrue(sql"DROP INDEX idx_event_id;".command)
      verifyTrue(sql"DROP INDEX idx_project_id;".command)

    }
  }

  private trait TestCase {
    val logger       = TestLogger[IO]()
    val tableCreator = new EventPayloadTableCreatorImpl[IO](sessionResource, logger)
  }
}
