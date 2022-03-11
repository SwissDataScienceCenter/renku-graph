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

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import io.renku.eventlog.InMemoryEventLogDb
import io.renku.testtools.IOSpec
import org.scalatest.{BeforeAndAfter, Suite}
import skunk._
import skunk.codec.all._
import skunk.implicits._

trait DbInitSpec extends InMemoryEventLogDb with EventLogDbMigrations with BeforeAndAfter {
  self: Suite with IOSpec =>

  protected[init] val migrationsToRun: List[DbMigrator[IO]]

  before {
    findAllTables() foreach dropTable
    migrationsToRun.map(_.run()).sequence.unsafeRunSync()
  }

  private def findAllTables(): List[String] = execute {
    Kleisli { session =>
      val query: Query[Void, String] = sql"""
          SELECT DISTINCT tablename FROM pg_tables
          WHERE schemaname != 'pg_catalog'
            AND schemaname != 'information_schema'""".query(name)
      session.execute(query)
    }
  }

  protected def createEventTable(): Unit =
    List(eventLogTableCreator, batchDateAdder, eventLogTableRenamer)
      .map(_.run())
      .sequence
      .void
      .unsafeRunSync()
}
