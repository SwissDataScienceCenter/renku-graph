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

package io.renku.eventlog

import cats.syntax.all._
import ch.datascience.db.DbSpec
import doobie.implicits.toSqlInterpolator
import doobie.util.fragment.Fragment
import io.renku.eventlog.init.EventLogDbMigrations
import org.scalatest.TestSuite

import scala.language.reflectiveCalls

trait InMemoryEventLogDbSpec
    extends DbSpec
    with EventLogDbMigrations
    with InMemoryEventLogDb
    with EventLogDataProvisioning
    with EventDataFetching {
  self: TestSuite =>

  protected def initDb(): Unit =
    allMigrations.map(_.run()).sequence.void.unsafeRunSync()

  private def findAllTables(): List[String] = execute {
    sql"""|SELECT DISTINCT tablename FROM pg_tables
          |WHERE schemaname != 'pg_catalog'
          |  AND schemaname != 'information_schema'""".stripMargin
      .query[String]
      .to[List]
  }

  protected def prepareDbForTest(): Unit = execute {
    Fragment.const(s"TRUNCATE TABLE ${findAllTables().mkString(", ")} CASCADE").update.run.void
  }
}
