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

class TimestampZoneAdderSpec extends AnyWordSpec with DbInitSpec with should.Matchers {
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
    statusesProcessingTimeTableCreator,
    subscriberTableCreator,
    eventDeliveryTableCreator
  )

  private val timestampType   = "timestamp without time zone"
  private val timestamptzType = "timestamp with time zone"

  "run" should {

    "modify the type of the timestamp columns" in new TestCase {

      tableExists("event") shouldBe true

      verify("batch_date", timestampType)
      verify("create_date", timestampType)
      verify("event_date", timestampType)
      verify("execution_date", timestampType)
      verify("last_synced", timestampType)
      verify("latest_event_date", timestampType)

      tableRefactor.run().unsafeRunSync() shouldBe ((): Unit)

      verify("batch_date", timestamptzType)
      verify("create_date", timestamptzType)
      verify("event_date", timestamptzType)
      verify("execution_date", timestamptzType)
      verify("last_synced", timestamptzType)
      verify("latest_event_date", timestamptzType)

    }

    "do nothing if the timestamp are already timestampz" in new TestCase {

      tableExists("event") shouldBe true

      tableRefactor.run().unsafeRunSync() shouldBe ((): Unit)

      verify("batch_date", timestamptzType)
      verify("create_date", timestamptzType)
      verify("event_date", timestamptzType)
      verify("execution_date", timestamptzType)
      verify("last_synced", timestamptzType)
      verify("latest_event_date", timestamptzType)

      tableRefactor.run().unsafeRunSync() shouldBe ((): Unit)

      verify("batch_date", timestamptzType)
      verify("create_date", timestamptzType)
      verify("event_date", timestamptzType)
      verify("execution_date", timestamptzType)
      verify("last_synced", timestamptzType)
      verify("latest_event_date", timestamptzType)

      logger.loggedOnly(Info("Fields are already in timestamptz type"))

    }

  }

  private trait TestCase {
    val logger        = TestLogger[IO]()
    val tableRefactor = new TimestampZoneAdderImpl[IO](transactor, logger)
  }
}
