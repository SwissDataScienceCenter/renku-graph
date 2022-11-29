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

package io.renku.tokenrepository.repository

import AccessTokenCrypto.EncryptedAccessToken
import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import io.renku.db.DbSpec
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.localDates
import io.renku.graph.model.projects.{Id, Path}
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.association.TokenDates.ExpiryDate
import io.renku.tokenrepository.repository.init.DbMigrations
import org.scalamock.scalatest.MockFactory
import org.scalatest.Suite
import skunk._
import skunk.codec.all._
import skunk.data.Completion
import skunk.implicits._

import java.time.{LocalDate, OffsetDateTime}

trait InMemoryProjectsTokensDbSpec extends DbSpec with InMemoryProjectsTokensDb with DbMigrations {
  self: Suite with IOSpec with MockFactory =>

  protected def initDb(): Unit =
    allMigrations.map(_.run()).sequence.void.unsafeRunSync()

  protected def prepareDbForTest(): Unit = execute {
    Kleisli[IO, Session[IO], Unit] { session =>
      val query: Command[skunk.Void] = sql"TRUNCATE TABLE projects_tokens".command
      session.execute(query).void
    }
  }

  protected def insert(projectId:      Id,
                       projectPath:    Path,
                       encryptedToken: EncryptedAccessToken,
                       expiryDate:     ExpiryDate = localDates(min = LocalDate.now().plusDays(1)).generateAs(ExpiryDate)
  ): Unit = execute {
    Kleisli[IO, Session[IO], Unit] { session =>
      val query: Command[Int ~ String ~ String ~ OffsetDateTime ~ LocalDate] =
        sql"""insert into projects_tokens (project_id, project_path, token, created_at, expiry_date)
              values ($int4, $varchar, $varchar, $timestamptz, $date)
         """.command
      session
        .prepare(query)
        .use(
          _.execute(
            projectId.value ~ projectPath.value ~ encryptedToken.value ~ OffsetDateTime.now() ~ expiryDate.value
          )
        )
        .map(assureInserted)
    }
  }

  private lazy val assureInserted: Completion => Unit = {
    case Completion.Insert(1) => ()
    case _                    => fail("insertion problem")
  }

  protected def findToken(projectPath: Path): Option[String] = sessionResource
    .useK {
      val query: Query[String, String] = sql"select token from projects_tokens where project_path = $varchar"
        .query(varchar)
      Kleisli(_.prepare(query).use(_.option(projectPath.value)))
    }
    .unsafeRunSync()

  protected def findToken(projectId: Id): Option[String] = sessionResource
    .useK {
      val query: Query[Int, String] = sql"select token from projects_tokens where project_id = $int4"
        .query(varchar)
      Kleisli(_.prepare(query).use(_.option(projectId.value)))
    }
    .unsafeRunSync()
}
