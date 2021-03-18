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

package ch.datascience.tokenrepository.repository

import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.SessionResource
import com.dimafeng.testcontainers._
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.util.fragment.Fragment
import doobie.util.transactor.Transactor
import org.scalatest.Suite
import org.testcontainers.utility.DockerImageName

import scala.concurrent.ExecutionContext.Implicits.global

trait InMemoryProjectsTokensDb extends ForAllTestContainer {
  self: Suite =>

  implicit val contextShift: ContextShift[IO] = IO.contextShift(global)

  private val dbConfig = new ProjectsTokensDbConfigProvider[IO].get().unsafeRunSync()

  override val container: Container with JdbcDatabaseContainer = PostgreSQLContainer(
    dockerImageNameOverride = DockerImageName.parse("postgres:9.6.19-alpine"),
    databaseName = "projects_tokens",
    username = dbConfig.user.value,
    password = dbConfig.pass
  )

  lazy val transactor: SessionResource[IO, ProjectsTokensDB] = DbTransactor[IO, ProjectsTokensDB] {
    Transactor.fromDriverManager[IO](
      dbConfig.driver.value,
      container.jdbcUrl,
      dbConfig.user.value,
      dbConfig.pass
    )
  }

  def execute[O](query: ConnectionIO[O]): O =
    query
      .transact(transactor.resource)
      .unsafeRunSync()

  protected def tableExists(): Boolean =
    sql"""select exists (select * from projects_tokens);""".query.option
      .transact(transactor.resource)
      .recover { case _ => None }
      .unsafeRunSync()
      .isDefined

  protected def createTable(): Unit = execute {
    sql"""
         |CREATE TABLE projects_tokens(
         | project_id int4 PRIMARY KEY,
         | project_path VARCHAR NOT NULL,
         | token VARCHAR NOT NULL
         |);
       """.stripMargin.update.run.map(_ => ())
  }

  protected def dropTable(): Unit = execute {
    sql"DROP TABLE IF EXISTS projects_tokens".update.run.map(_ => ())
  }

  protected def verifyTrue(sql: Fragment): Unit = execute {
    sql.update.run.map(_ => ())
  }
}
