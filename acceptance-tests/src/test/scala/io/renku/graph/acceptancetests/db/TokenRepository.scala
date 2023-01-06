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

package io.renku.graph.acceptancetests.db

import cats.Applicative
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import io.renku.db.{DBConfigProvider, PostgresContainer}
import io.renku.tokenrepository.repository.{ProjectsTokensDB, ProjectsTokensDbConfigProvider}
import org.typelevel.log4cats.Logger

import scala.util.Try

object TokenRepository {

  private lazy val dbConfig: DBConfigProvider.DBConfig[ProjectsTokensDB] =
    new ProjectsTokensDbConfigProvider[Try].get().fold(throw _, identity)

  private lazy val postgresContainer = FixedHostPortGenericContainer(
    imageName = PostgresContainer.image,
    env = Map("POSTGRES_USER"     -> dbConfig.user.value,
              "POSTGRES_PASSWORD" -> dbConfig.pass.value,
              "POSTGRES_DB"       -> dbConfig.name.value
    ),
    exposedPorts = Seq(dbConfig.port.value),
    exposedHostPort = dbConfig.port.value,
    exposedContainerPort = dbConfig.port.value,
    command = Seq(s"-p ${dbConfig.port.value}")
  )

  def startDB()(implicit ioRuntime: IORuntime, logger: Logger[IO]): IO[Unit] = for {
    _ <- Applicative[IO].unlessA(postgresContainer.container.isRunning)(IO(postgresContainer.start()))
    _ <- logger.info("projects_tokens DB started")
  } yield ()
}
