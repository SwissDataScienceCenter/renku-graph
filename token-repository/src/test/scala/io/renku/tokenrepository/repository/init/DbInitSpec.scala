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

package io.renku.tokenrepository.repository.init

import cats.effect.IO
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.db.SessionResource
import io.renku.graph.model.projects.{GitLabId, Slug}
import io.renku.http.client.GitLabClient
import io.renku.interpreters.TestLogger
import io.renku.tokenrepository.repository.creation.TokenDates.ExpiryDate
import io.renku.tokenrepository.repository.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.tokenrepository.repository.{ProjectsTokensDB, TokenRepositoryPostgresSpec}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Suite
import skunk._
import skunk.codec.all._
import skunk.data.Completion
import skunk.implicits._

import java.time.LocalDate

trait DbInitSpec extends TokenRepositoryPostgresSpec {
  self: Suite with AsyncMockFactory =>

  implicit lazy val logger: TestLogger[IO] = TestLogger()

  protected val runMigrationsUpTo: Class[_ <: DBMigration[IO]]

  override lazy val migrations: SessionResource[IO, ProjectsTokensDB] => IO[Unit] = { sr =>
    implicit val gl:  GitLabClient[IO]                     = mock[GitLabClient[IO]]
    implicit val qet: QueriesExecutionTimes[IO]            = TestQueriesExecutionTimes[IO]
    implicit val msr: ProjectsTokensDB.SessionResource[IO] = ProjectsTokensDB.SessionResource[IO](sr)
    DbInitializer
      .migrations[IO]
      .flatMap {
        _.takeWhile(m => !runMigrationsUpTo.isAssignableFrom(m.getClass))
          .traverse_(_.run)
      } >> logger.resetF()
  }

  protected def findToken(projectSlug: Slug)(implicit cfg: DBConfig[ProjectsTokensDB]): IO[Option[String]] =
    sessionResource(cfg).use { session =>
      val query: Query[String, String] =
        sql"select token from projects_tokens where project_path = $varchar".query(varchar)
      session.prepare(query).flatMap(_.option(projectSlug.value))
    }

  protected def findExpiryDate(projectId: GitLabId)(implicit cfg: DBConfig[ProjectsTokensDB]): IO[Option[ExpiryDate]] =
    sessionResource(cfg).use { session =>
      val query: Query[Int, ExpiryDate] = sql"SELECT expiry_date FROM projects_tokens WHERE project_id = $int4"
        .query(date)
        .map { case expiryDate: LocalDate => ExpiryDate(expiryDate) }
      session.prepare(query).flatMap(_.option(projectId.value))
    }

  protected lazy val assureInserted: Completion => Unit = {
    case Completion.Insert(1) => ()
    case _                    => fail("insertion problem")
  }

  protected def tableExists(tableName: String)(implicit cfg: DBConfig[ProjectsTokensDB]): IO[Boolean] =
    sessionResource(cfg).use { session =>
      val query: Query[String, Boolean] =
        sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = $varchar)".query(bool)
      session.prepare(query).flatMap(_.unique(tableName)).recover { case _ => false }
    }

  protected def verifyColumnExists(table: String, column: String)(implicit
      cfg: DBConfig[ProjectsTokensDB]
  ): IO[Boolean] =
    sessionResource(cfg).use { session =>
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

  protected def verifyColumnNullable(table: String, column: String)(implicit
      cfg: DBConfig[ProjectsTokensDB]
  ): IO[Boolean] =
    sessionResource(cfg).use { session =>
      val query: Query[String *: String *: EmptyTuple, Boolean] =
        sql""" SELECT DISTINCT is_nullable
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

  def verifyIndexExists(table: String, indexName: String)(implicit
      cfg: DBConfig[ProjectsTokensDB]
  ): IO[Boolean] =
    sessionResource(cfg).use { session =>
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
