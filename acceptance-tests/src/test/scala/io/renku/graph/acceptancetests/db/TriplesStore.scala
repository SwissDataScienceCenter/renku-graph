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

import cats.effect.{IO, Resource, Temporal}
import cats.{Applicative, Monad}
import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import eu.timepit.refined.auto._
import io.renku.db.{DBConfigProvider, PostgresContainer}
import io.renku.triplesgenerator.TgLockDB.SessionResource
import io.renku.triplesgenerator.{TgLockDB, TgLockDbConfigProvider}
import io.renku.triplesstore._
import natchez.Trace.Implicits.noop
import org.typelevel.log4cats.Logger
import skunk.Session

import scala.concurrent.duration._
import scala.util.Try

object TriplesStore extends InMemoryJena with ProjectsDataset with MigrationsDataset {

  protected override val jenaRunMode: JenaRunMode = JenaRunMode.FixedPortContainer(3030)

  private lazy val dbConfig: DBConfigProvider.DBConfig[TgLockDB] =
    new TgLockDbConfigProvider[Try].get().fold(throw _, identity)

  private lazy val postgresContainer = FixedHostPortGenericContainer(
    imageName = PostgresContainer.image,
    env = Map(
      "POSTGRES_USER"     -> dbConfig.user.value,
      "POSTGRES_PASSWORD" -> dbConfig.pass.value,
      "POSTGRES_DB"       -> dbConfig.name.value
    ),
    exposedPorts = Seq(dbConfig.port.value),
    exposedHostPort = dbConfig.port.value,
    exposedContainerPort = dbConfig.port.value,
    command = Seq(s"-p ${dbConfig.port.value}")
  )

  lazy val sessionResource: Resource[IO, SessionResource[IO]] =
    Session
      .pooled[IO](
        host = postgresContainer.host,
        port = dbConfig.port.value,
        database = dbConfig.name.value,
        user = dbConfig.user.value,
        password = Some(dbConfig.pass.value),
        max = dbConfig.connectionPool.value
      )
      .map(new SessionResource(_))

  def start()(implicit logger: Logger[IO]): IO[Unit] = for {
    _ <- Applicative[IO].unlessA(isRunning)(IO(container.start()))
    _ <- Applicative[IO].unlessA(postgresContainer.container.isRunning)(IO(postgresContainer.start()))
    _ <- waitForReadiness
    _ <- logger.info("Triples Store started")
  } yield ()

  private def isRunning: Boolean = Option(container.containerInfo).exists(_.getState.getRunning)

  private def waitForReadiness(implicit logger: Logger[IO]): IO[Unit] =
    Monad[IO].whileM_(IO(!isRunning))(logger.info("Waiting for TS") >> (Temporal[IO] sleep (500 millis)))

}
