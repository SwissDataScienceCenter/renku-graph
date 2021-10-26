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

package io.renku.graph.acceptancetests.db

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import io.renku.db.DBConfigProvider
import io.renku.graph.acceptancetests.tooling.TestLogger
import io.renku.tokenrepository.repository.{ProjectsTokensDB, ProjectsTokensDbConfigProvider}

object TokenRepository {

  private val logger = TestLogger()

  private def dbConfig(implicit ioRuntime: IORuntime): DBConfigProvider.DBConfig[ProjectsTokensDB] =
    new ProjectsTokensDbConfigProvider[IO].get().unsafeRunSync()

  private def postgresContainer(implicit ioRuntime: IORuntime) = FixedHostPortGenericContainer(
    imageName = "postgres:11.11-alpine",
    env = Map("POSTGRES_USER"     -> dbConfig.user.value,
              "POSTGRES_PASSWORD" -> dbConfig.pass.value,
              "POSTGRES_DB"       -> dbConfig.name.value
    ),
    exposedPorts = Seq(dbConfig.port.value),
    exposedHostPort = dbConfig.port.value,
    exposedContainerPort = dbConfig.port.value,
    command = Seq(s"-p ${dbConfig.port.value}")
  )

  def startDB()(implicit ioRuntime: IORuntime): IO[Unit] = for {
    _ <- IO(postgresContainer.start())
    _ <- logger.info("projects_tokens DB started")
  } yield ()

  def stopDB()(implicit ioRuntime: IORuntime): IO[Unit] = for {
    _ <- IO(postgresContainer.stop())
    _ <- logger.info("projects_tokens DB stopped")
  } yield ()
}
