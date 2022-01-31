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

  "run" should {

    "modify the type of the timestamp columns" in new TestCase {

      tableExists("event") shouldBe true

      columnsToMigrate foreach { case (table, column) =>
        verify(table, column, timestampType)
      }

      tableRefactor.run().unsafeRunSync() shouldBe ((): Unit)

      columnsToMigrate foreach { case (table, column) =>
        verify(table, column, timestamptzType)
      }

      val expectedLogs = columnsToMigrate map { case (table, column) =>
        Info(s"$table.$column in 'timestamp without time zone', migrating")
      }
      logger.loggedOnly(expectedLogs: _*)
    }

    "do nothing if the timestamps are already timestampz" in new TestCase {

      tableExists("event") shouldBe true

      tableRefactor.run().unsafeRunSync() shouldBe ()

      columnsToMigrate foreach { case (table, column) =>
        verify(table, column, timestamptzType)
      }

      tableRefactor.run().unsafeRunSync() shouldBe ()

      columnsToMigrate foreach { case (table, column) =>
        verify(table, column, timestamptzType)
      }

      val migrationLogs = columnsToMigrate map { case (table, column) =>
        Info(s"$table.$column in 'timestamp without time zone', migrating")
      }
      val secondMigrationLogs = columnsToMigrate map { case (table, column) =>
        Info(s"$table.$column already migrated to 'timestamp with time zone'")
      }
      logger.loggedOnly(migrationLogs ::: secondMigrationLogs: _*)
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val tableRefactor = new TimestampZoneAdderImpl[IO](sessionResource)
  }

  private lazy val timestampType   = "timestamp without time zone"
  private lazy val timestamptzType = "timestamp with time zone"

  private lazy val columnsToMigrate = List(
    "event"                           -> "batch_date",
    "event"                           -> "created_date",
    "event"                           -> "execution_date",
    "event"                           -> "event_date",
    "subscription_category_sync_time" -> "last_synced",
    "project"                         -> "latest_event_date"
  )
}
