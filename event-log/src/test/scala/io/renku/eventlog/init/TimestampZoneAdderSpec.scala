/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TimestampZoneAdderSpec extends AnyWordSpec with IOSpec with DbInitSpec with should.Matchers {

  protected override lazy val migrationsToRun: List[Migration] = allMigrations.takeWhile {
    case _: TimestampZoneAdderImpl[_] => false
    case _ => true
  }

  private val timestampType   = "timestamp without time zone"
  private val timestamptzType = "timestamp with time zone"

  "run" should {

    "modify the type of the timestamp columns" in new TestCase {

      tableExists("event") shouldBe true

      verify("event", "batch_date", timestampType)
      verify("event", "create_date", timestampType)
      verify("event", "event_date", timestampType)
      verify("event", "execution_date", timestampType)
      verify("event", "last_synced", timestampType)
      verify("event", "latest_event_date", timestampType)

      tableRefactor.run().unsafeRunSync() shouldBe ((): Unit)

      verify("event", "batch_date", timestamptzType)
      verify("event", "create_date", timestamptzType)
      verify("event", "event_date", timestamptzType)
      verify("event", "execution_date", timestamptzType)
      verify("event", "last_synced", timestamptzType)
      verify("event", "latest_event_date", timestamptzType)

    }

    "do nothing if the timestamp are already timestampz" in new TestCase {

      tableExists("event") shouldBe true

      tableRefactor.run().unsafeRunSync() shouldBe ((): Unit)

      verify("event", "batch_date", timestamptzType)
      verify("event", "create_date", timestamptzType)
      verify("event", "event_date", timestamptzType)
      verify("event", "execution_date", timestamptzType)
      verify("event", "last_synced", timestamptzType)
      verify("event", "latest_event_date", timestamptzType)

      tableRefactor.run().unsafeRunSync() shouldBe ((): Unit)

      verify("event", "batch_date", timestamptzType)
      verify("event", "create_date", timestamptzType)
      verify("event", "event_date", timestamptzType)
      verify("event", "execution_date", timestamptzType)
      verify("event", "last_synced", timestamptzType)
      verify("event", "latest_event_date", timestamptzType)

      logger.loggedOnly(Info("Fields are already in timestamptz type"))
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val tableRefactor = new TimestampZoneAdderImpl[IO](sessionResource)
  }
}
