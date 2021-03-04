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
import doobie.implicits.toSqlInterpolator
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SubscriberTableCreatorSpec extends AnyWordSpec with DbInitSpec with should.Matchers {

  protected override lazy val migrationsToRun: List[Migration] = List(
    eventLogTableCreator,
    projectPathAdder,
    batchDateAdder,
    latestEventDatesViewRemover,
    projectTableCreator,
    projectPathRemover,
    eventLogTableRenamer,
    eventStatusRenamer,
    eventPayloadTableCreator,
    eventPayloadSchemaVersionAdder,
    subscriptionCategorySyncTimeTableCreator,
    statusesProcessingTimeTableCreator
  )

  "run" should {

    "do nothing if the 'subscriber' table already exists" in new TestCase {

      tableExists("subscriber") shouldBe false

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("subscriber") shouldBe true

      logger.loggedOnly(Info("'subscriber' table created"))

      logger.reset()

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("'subscriber' table exists"))
    }

    "create indices for all the columns" in new TestCase {

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("subscriber") shouldBe true

      verifyTrue(sql"DROP INDEX idx_delivery_id")
      verifyTrue(sql"DROP INDEX idx_delivery_url")
      verifyTrue(sql"DROP INDEX idx_source_url")
    }
  }

  private trait TestCase {
    val logger       = TestLogger[IO]()
    val tableCreator = new SubscriberTableCreatorImpl[IO](transactor, logger)
  }
}
