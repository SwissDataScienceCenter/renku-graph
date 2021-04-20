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

import cats.data.Kleisli
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.SessionResource
import com.dimafeng.testcontainers._
import natchez.Trace.Implicits.noop
import org.scalatest.Suite
import org.testcontainers.utility.DockerImageName
import skunk._
import skunk.codec.all._
import skunk.implicits._

import scala.concurrent.ExecutionContext.Implicits.global

trait InMemoryProjectsTokensDb extends ForAllTestContainer {
  self: Suite =>

  implicit val contextShift: ContextShift[IO] = IO.contextShift(global)

  private val dbConfig = new ProjectsTokensDbConfigProvider[IO].get().unsafeRunSync()

  override val container: PostgreSQLContainer = PostgreSQLContainer(
    dockerImageNameOverride = DockerImageName.parse("postgres:9.6.19-alpine"),
    databaseName = "projects_tokens",
    username = dbConfig.user.value,
    password = dbConfig.pass.value
  )

  lazy val sessionResource: SessionResource[IO, ProjectsTokensDB] = new SessionResource[IO, ProjectsTokensDB](
    Session.single(
      host = container.host,
      database = dbConfig.name.value,
      user = dbConfig.user.value,
      password = Some(dbConfig.pass.value),
      port = container.container.getMappedPort(dbConfig.port.value)
    )
  )

  def execute[O](query: Kleisli[IO, Session[IO], O]): O =
    sessionResource.useK(query).unsafeRunSync()

  protected def tableExists(): Boolean =
    sessionResource
      .useK {
        val query: Query[Void, Boolean] = sql"""select exists (select * from projects_tokens);""".query(bool)
        Kleisli[IO, Session[IO], Option[Boolean]](_.option(query).recover { case _ => None })
      }
      .unsafeRunSync()
      .isDefined

  protected def createTable(): Unit = execute {
    Kleisli[IO, Session[IO], Unit] { session =>
      val query: Command[Void] =
        sql"""CREATE TABLE projects_tokens(
              project_id int4 PRIMARY KEY,
              project_path VARCHAR NOT NULL,
              token VARCHAR NOT NULL
             );
        """.command
      session.execute(query).map(_ => ())
    }
  }

  protected def dropTable(): Unit = execute {
    Kleisli[IO, Session[IO], Unit] { session =>
      val query: Command[Void] = sql"DROP TABLE IF EXISTS projects_tokens".command
      session.execute(query).map(_ => ())
    }
  }

  protected def verifyTrue(sql: Command[Void]): Unit = execute {
    Kleisli[IO, Session[IO], Unit] { session =>
      session.execute(sql).map(_ => ())
    }
  }
}
