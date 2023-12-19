/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.EventLogDB
import io.renku.interpreters.TestLogger.Level.Info
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class TimestampZoneAdderSpec extends AsyncFlatSpec with AsyncIOSpec with DbInitSpec with should.Matchers {

  protected[init] override val runMigrationsUpTo: Class[_ <: DbMigrator[IO]] = classOf[TimestampZoneAdder[IO]]

  it should "modify the type of the timestamp columns" in testDBResource.use { implicit cfg =>
    for {
      _ <- tableExists("event").asserting(_ shouldBe true)

      _ <- columnsToMigrate.traverse_ { case (table, column) =>
             verifyExists(table, column, timestampType).asserting(_ shouldBe true)
           }

      _ <- tableRefactor.run.assertNoException

      _ <- columnsToMigrate.traverse_ { case (table, column) =>
             verifyExists(table, column, timestamptzType).asserting(_ shouldBe true)
           }

      expectedLogs = columnsToMigrate map { case (table, column) =>
                       Info(s"$table.$column in 'timestamp without time zone', migrating")
                     }
      _ <- logger.loggedOnlyF(expectedLogs: _*)
    } yield Succeeded
  }

  it should "do nothing if the timestamps are already timestampz" in testDBResource.use { implicit cfg =>
    for {
      _ <- tableExists("event").asserting(_ shouldBe true)

      _ <- tableRefactor.run.assertNoException

      _ <- columnsToMigrate.traverse_ { case (table, column) =>
             verifyExists(table, column, timestamptzType).asserting(_ shouldBe true)
           }

      _ <- tableRefactor.run.assertNoException

      _ <- columnsToMigrate.traverse_ { case (table, column) =>
             verifyExists(table, column, timestamptzType).asserting(_ shouldBe true)
           }

      migrationLogs = columnsToMigrate map { case (table, column) =>
                        Info(s"$table.$column in 'timestamp without time zone', migrating")
                      }
      secondMigrationLogs = columnsToMigrate map { case (table, column) =>
                              Info(s"$table.$column already migrated to 'timestamp with time zone'")
                            }
      _ <- logger.loggedOnlyF(migrationLogs ::: secondMigrationLogs: _*)
    } yield Succeeded
  }

  private def tableRefactor(implicit cfg: DBConfig[EventLogDB]) = new TimestampZoneAdderImpl[IO]

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
