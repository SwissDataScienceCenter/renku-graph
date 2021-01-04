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
import doobie.implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventPayloadSchemaVersionAdderSpec extends AnyWordSpec with DbInitSpec with should.Matchers {

  protected override lazy val migrationsToRun: List[Migration] = List(
    eventLogTableCreator,
    projectPathAdder,
    batchDateAdder,
    latestEventDatesViewRemover,
    projectTableCreator,
    projectPathRemover,
    eventLogTableRenamer,
    eventStatusRenamer,
    eventPayloadTableCreator
  )

  "run" should {
    "fail if the 'event_payload' table does not exist" in new TestCase {
      dropTable("event_payload")
      tableExists("event_payload") shouldBe false
      intercept[Exception] {

        tableCreator.run().unsafeRunSync()
      }.getMessage shouldBe "Event payload table missing; alteration is not possible"
    }

    "add a column for the schema version" in new TestCase {

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      verifyTrue(sql"ALTER TABLE event_payload DROP COLUMN schema_version;")
    }

    "create indices for certain columns" in new TestCase {

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      verifyTrue(sql"DROP INDEX idx_event_id;")
      verifyTrue(sql"DROP INDEX idx_project_id;")
      verifyTrue(sql"DROP INDEX idx_schema_version;")

    }
  }

  private trait TestCase {
    val logger       = TestLogger[IO]()
    val tableCreator = new EventPayloadSchemaVersionAdderImpl[IO](transactor, logger)
  }
}
