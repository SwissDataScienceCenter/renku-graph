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

import AccessTokenCrypto.EncryptedAccessToken
import cats.effect.IO
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.db.{PostgresServer, PostgresSpec, SessionResource}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.localDates
import io.renku.graph.model.projects.{GitLabId, Slug}
import io.renku.http.client.GitLabClient
import io.renku.interpreters.TestLogger
import io.renku.tokenrepository.repository.creation.TokenDates.ExpiryDate
import io.renku.tokenrepository.repository.init.DbInitializer
import io.renku.tokenrepository.repository.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Suite
import skunk._
import skunk.codec.all._
import skunk.data.Completion
import skunk.implicits._

import java.time.{LocalDate, OffsetDateTime}

trait TokenRepositoryPostgresSpec extends PostgresSpec[ProjectsTokensDB] with TokenRepositoryTypeSerializers {
  self: Suite with AsyncMockFactory =>

  lazy val server: PostgresServer = TokenRepositoryPostgresServer

  lazy val migrations: SessionResource[IO, ProjectsTokensDB] => IO[Unit] = { sr =>
    implicit lazy val logger:           TestLogger[IO]                       = TestLogger[IO]()
    implicit lazy val glClient:         GitLabClient[IO]                     = mock[GitLabClient[IO]]
    implicit val qet:                   QueriesExecutionTimes[IO]            = TestQueriesExecutionTimes[IO]
    implicit val moduleSessionResource: ProjectsTokensDB.SessionResource[IO] = ProjectsTokensDB.SessionResource[IO](sr)
    DbInitializer.migrations[IO].flatMap(_.map(_.run).sequence).void
  }

  implicit def moduleSessionResource(implicit cfg: DBConfig[ProjectsTokensDB]): ProjectsTokensDB.SessionResource[IO] =
    io.renku.db.SessionResource[IO, ProjectsTokensDB](sessionResource(cfg), cfg)

  protected def insert(projectId:      GitLabId,
                       projectSlug:    Slug,
                       encryptedToken: EncryptedAccessToken,
                       expiryDate:     ExpiryDate = localDates(min = LocalDate.now().plusDays(1)).generateAs(ExpiryDate)
  )(implicit cfg: DBConfig[ProjectsTokensDB]): IO[Unit] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Command[Int *: String *: String *: OffsetDateTime *: LocalDate *: EmptyTuple] =
        sql"""INSERT into projects_tokens (project_id, project_slug, token, created_at, expiry_date)
              VALUES ($int4, $varchar, $varchar, $timestamptz, $date)
         """.command
      session
        .prepare(query)
        .flatMap(
          _.execute(
            projectId.value *:
              projectSlug.value *:
              encryptedToken.value *:
              OffsetDateTime.now() *:
              expiryDate.value *:
              EmptyTuple
          )
        )
        .map {
          case Completion.Insert(1) => ()
          case c                    => fail(s"insertion problem: $c")
        }
    }

  protected def findToken(projectId: GitLabId)(implicit cfg: DBConfig[ProjectsTokensDB]): IO[Option[String]] =
    moduleSessionResource(cfg).session
      .use { session =>
        val query: Query[Int, String] = sql"select token from projects_tokens where project_id = $int4".query(varchar)
        session.prepare(query).flatMap(_.option(projectId.value))
      }
}
