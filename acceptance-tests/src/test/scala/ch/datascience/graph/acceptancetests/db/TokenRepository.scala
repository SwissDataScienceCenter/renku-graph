/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.acceptancetests.db

import cats.effect.IO
import ch.datascience.db.DBConfigProvider
import ch.datascience.graph.acceptancetests.tooling.TestLogger
import ch.datascience.tokenrepository.repository.{ProjectsTokensDB, ProjectsTokensDbConfigProvider}
import com.dimafeng.testcontainers.{Container, JdbcDatabaseContainer, PostgreSQLContainer}

object TokenRepository {

  private val logger = TestLogger()

  private val dbConfig: DBConfigProvider.DBConfig[ProjectsTokensDB] =
    new ProjectsTokensDbConfigProvider[IO].get().unsafeRunSync()

  private val postgresContainer: Container with JdbcDatabaseContainer = PostgreSQLContainer(
    dockerImageNameOverride = "postgres:9.6.19-alpine",
    databaseName = "projects_tokens",
    username = dbConfig.user.value,
    password = dbConfig.pass
  )

  lazy val jdbcUrl: String = postgresContainer.jdbcUrl

  def startDB(): IO[Unit] = for {
    _ <- IO(postgresContainer.start())
    _ <- logger.info("projects_tokens DB started")
  } yield ()
}
