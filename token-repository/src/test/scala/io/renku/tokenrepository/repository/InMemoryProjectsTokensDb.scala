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

package io.renku.tokenrepository.repository

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.dimafeng.testcontainers._
import io.renku.db.PostgresContainer
import io.renku.tokenrepository.repository.ProjectsTokensDB.SessionResource
import natchez.Trace.Implicits.noop
import org.scalatest.Suite
import skunk._
import skunk.codec.all._
import skunk.implicits._

trait InMemoryProjectsTokensDb extends ForAllTestContainer with TokenRepositoryTypeSerializers {
  self: Suite =>

  implicit val ioRuntime: IORuntime

  private val dbConfig = new ProjectsTokensDbConfigProvider[IO].get().unsafeRunSync()

  override val container: PostgreSQLContainer = PostgresContainer.container(dbConfig)

  implicit lazy val sessionResource: SessionResource[IO] = io.renku.db.SessionResource[IO, ProjectsTokensDB](
    Session.single(
      host = container.host,
      database = dbConfig.name.value,
      user = dbConfig.user.value,
      password = Some(dbConfig.pass.value),
      port = container.container.getMappedPort(dbConfig.port.value)
    )
  )

  def execute[O](query: Kleisli[IO, Session[IO], O]): O =
    SessionResource[IO].useK(query).unsafeRunSync()

  protected def tableExists(tableName: String): Boolean = execute[Boolean] {
    Kleisli { session =>
      val query: Query[String, Boolean] =
        sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = $varchar)".query(bool)
      session.prepare(query).flatMap(_.unique(tableName)).recover { case _ => false }
    }
  }

  protected def dropTable(table: String): Unit = execute {
    Kleisli[IO, Session[IO], Unit] { session =>
      val query: Command[Void] = sql"DROP TABLE IF EXISTS #$table".command
      session.execute(query).void
    }
  }

  protected def verifyColumnExists(table: String, column: String): Boolean = execute[Boolean] {
    Kleisli { session =>
      val query: Query[String *: String *: EmptyTuple, Boolean] =
        sql"""SELECT EXISTS (
                SELECT *
                FROM information_schema.columns
                WHERE table_name = $varchar AND column_name = $varchar
              )""".query(bool)
      session
        .prepare(query)
        .flatMap(_.unique(table *: column *: EmptyTuple))
        .recover { case _ => false }
    }
  }

  protected def verifyColumnNullable(table: String, column: String): Boolean = execute[Boolean] {
    Kleisli { session =>
      val query: Query[String *: String *: EmptyTuple, Boolean] = sql"""
        SELECT DISTINCT is_nullable
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = $varchar AND column_name = $varchar"""
        .query(varchar(3))
        .map {
          case "YES" => true
          case "NO"  => false
          case other => throw new Exception(s"is_nullable for $table.$column has value $other")
        }
      session
        .prepare(query)
        .flatMap(_.unique(table *: column *: EmptyTuple))
    }
  }

  def verifyIndexExists(table: String, indexName: String): Boolean = execute[Boolean] {
    Kleisli { session =>
      val query: Query[String *: String *: EmptyTuple, Boolean] =
        sql"""SELECT EXISTS (
               SELECT *
               FROM pg_indexes
               WHERE tablename = $varchar AND indexname = $varchar
             )""".query(bool)
      session
        .prepare(query)
        .flatMap(_.unique(table *: indexName *: EmptyTuple))
        .recover { case _ => false }
    }
  }

  protected def verifyTrue(sql: Command[Void]): Unit = execute {
    Kleisli[IO, Session[IO], Unit] { session =>
      session.execute(sql).void
    }
  }
}
