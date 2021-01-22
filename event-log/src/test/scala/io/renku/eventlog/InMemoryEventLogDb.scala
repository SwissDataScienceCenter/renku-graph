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

import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.DbTransactor
import com.dimafeng.testcontainers._
import doobie.Transactor
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.util.fragment.Fragment
import org.scalatest.Suite
import org.testcontainers.utility.DockerImageName

import scala.concurrent.ExecutionContext.Implicits.global

trait InMemoryEventLogDb extends ForAllTestContainer with TypeSerializers {
  self: Suite =>

  object Tables {
    val event                        = "event"
    val project                      = "project"
    val eventPayload                 = "event_payload"
    val subscriptionCategorySyncTime = "subscription_category_sync_time"
    lazy val all                     = Set(event, eventPayload, subscriptionCategorySyncTime, project)
  }

  implicit val contextShift: ContextShift[IO] = IO.contextShift(global)

  private val dbConfig = new EventLogDbConfigProvider[IO].get().unsafeRunSync()

  override val container: Container with JdbcDatabaseContainer = PostgreSQLContainer(
    dockerImageNameOverride = DockerImageName.parse("postgres:9.6.19-alpine"),
    databaseName = "event_log",
    username = dbConfig.user.value,
    password = dbConfig.pass
  )

  lazy val transactor: DbTransactor[IO, EventLogDB] = DbTransactor[IO, EventLogDB] {
    Transactor.fromDriverManager[IO](
      dbConfig.driver.value,
      container.jdbcUrl,
      dbConfig.user.value,
      dbConfig.pass
    )
  }

  def execute[O](query: ConnectionIO[O]): O =
    query
      .transact(transactor.get)
      .unsafeRunSync()

  def verifyTrue(sql: Fragment): Unit = execute {
    sql.update.run.map(_ => ())
  }

  def tableExists(tableName: String): Boolean =
    sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = $tableName)"
      .query[Boolean]
      .unique
      .transact(transactor.get)
      .recover { case _ => false }
      .unsafeRunSync()

  def viewExists(viewName: String): Boolean =
    Fragment
      .const(s"select exists (select * from $viewName)")
      .query[Boolean]
      .unique
      .transact(transactor.get)
      .recover { case _ => false }
      .unsafeRunSync()

  def dropTable(tableName: String): Unit = execute {
    Fragment.const(s"DROP TABLE IF EXISTS $tableName").update.run.map(_ => ())
  }
}
