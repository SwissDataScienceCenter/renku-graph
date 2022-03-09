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

package io.renku.eventlog

import cats.data.Kleisli
import cats.syntax.all._
import io.renku.db.DbSpec
import io.renku.eventlog.init.EventLogDbMigrations
import io.renku.testtools.IOSpec
import org.scalatest.TestSuite
import skunk._
import skunk.codec.all._
import skunk.implicits._

trait InMemoryEventLogDbSpec
    extends DbSpec
    with EventLogDbMigrations
    with InMemoryEventLogDb
    with EventLogDataProvisioning
    with EventDataFetching {
  self: TestSuite with IOSpec =>

  protected def initDb(): Unit =
    allMigrations.map(_.run()).sequence.void.unsafeRunSync()

  private def findAllTables(): List[String] = execute {
    Kleisli { session =>
      val query: Query[Void, String] = sql"""SELECT DISTINCT tablename FROM pg_tables
                                             WHERE schemaname != 'pg_catalog'
                                             AND schemaname != 'information_schema'"""
        .query(name)
      session.execute(query)
    }
  }

  protected def prepareDbForTest(): Unit = execute[Unit] {
    Kleisli { session =>
      val query: Command[Void] = sql"TRUNCATE TABLE #${findAllTables().mkString(", ")} CASCADE".command
      session.execute(query).void
    }
  }
}
