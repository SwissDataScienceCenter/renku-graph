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
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.EventLogDB
import io.renku.interpreters.TestLogger.Level.Info
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import skunk._
import skunk.implicits._

class EventLogTableRenamerSpec extends AsyncFlatSpec with AsyncIOSpec with DbInitSpec with should.Matchers {

  protected[init] override val runMigrationsUpTo: Class[_ <: DbMigrator[IO]] = classOf[EventLogTableRenamer[IO]]

  it should "rename the 'event_log' table to 'event' when 'event' does not exist" in testDBResource.use {
    implicit cfg =>
      for {
        _ <- tableExists("event_log").asserting(_ shouldBe true)

        _ <- tableRenamer.run.assertNoException

        _ <- tableExists("event_log").asserting(_ shouldBe false)
        _ <- tableExists("event").asserting(_ shouldBe true)

        _ <- logger.loggedOnlyF(Info("'event_log' table renamed to 'event'"))
      } yield Succeeded
  }

  it should "do nothing if the 'event' table already exists and 'event_log' does not exist" in testDBResource.use {
    implicit cfg =>
      for {
        _ <- tableExists("event_log").asserting(_ shouldBe true)

        _ <- tableRenamer.run.assertNoException

        _ <- tableExists("event_log").asserting(_ shouldBe false)
        _ <- tableExists("event").asserting(_ shouldBe true)

        _ <- logger.loggedOnlyF(Info("'event_log' table renamed to 'event'"))

        _ <- logger.resetF()

        _ <- tableRenamer.run.assertNoException

        _ <- logger.loggedOnlyF(Info("'event' table already exists"))
      } yield Succeeded
  }

  it should "drop 'event_log' table if both the 'event' and 'event_log' tables exist" in testDBResource.use {
    implicit cfg =>
      for {
        _ <- tableExists("event_log").asserting(_ shouldBe true)

        _ <- tableRenamer.run.assertNoException

        _ <- tableExists("event_log").asserting(_ shouldBe false)
        _ <- tableExists("event").asserting(_ shouldBe true)

        _ <- logger.loggedOnlyF(Info("'event_log' table renamed to 'event'"))

        _ <- logger.resetF()

        _ <- createEventLogTable

        _ <- tableExists("event_log").asserting(_ shouldBe true)
        _ <- tableExists("event").asserting(_ shouldBe true)

        _ <- tableRenamer.run.assertNoException

        _ <- tableExists("event_log").asserting(_ shouldBe false)
        _ <- tableExists("event").asserting(_ shouldBe true)

        _ <- logger.loggedOnlyF(Info("'event_log' table dropped"))
      } yield Succeeded
  }

  private def tableRenamer(implicit cfg: DBConfig[EventLogDB]) = new EventLogTableRenamerImpl[IO]

  private def createEventLogTable(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    moduleSessionResource.session.use { session =>
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
        )""".command
      session.execute(query).void
    }
}
