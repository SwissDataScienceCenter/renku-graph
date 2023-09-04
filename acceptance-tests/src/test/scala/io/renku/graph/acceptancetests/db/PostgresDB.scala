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

import cats.effect._
import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import eu.timepit.refined.auto._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.db.{PostgresContainer, SessionResource}
import skunk.codec.all._
import skunk.implicits._
import skunk._
import natchez.Trace.Implicits.noop
import org.typelevel.log4cats.Logger
import scala.concurrent.duration._

object PostgresDB {
  private[this] val starting: Ref[IO, Boolean] = Ref.unsafe[IO, Boolean](false)

  private val dbConfig = DBConfig[Any](
    host = "localhost",
    port = 5432,
    name = "postgres",
    user = "at",
    pass = "at",
    connectionPool = 8
  )

  private val postgresContainer = {
    val cfg = dbConfig
    FixedHostPortGenericContainer(
      imageName = PostgresContainer.image,
      env = Map(
        "POSTGRES_USER"     -> cfg.user.value,
        "POSTGRES_PASSWORD" -> cfg.pass.value
      ),
      exposedPorts = Seq(cfg.port.value),
      exposedHostPort = cfg.port.value,
      exposedContainerPort = cfg.port.value,
      command = Seq(s"-p ${cfg.port.value}")
    )
  }

  def startPostgres(implicit L: Logger[IO]) =
    starting.getAndUpdate(_ => true).flatMap {
      case false =>
        IO.unlessA(postgresContainer.container.isRunning)(
          IO(postgresContainer.start()) *> waitForPostgres *> L.info(
            "PostgreSQL database started"
          )
        )

      case true =>
        waitForPostgres
    }

  private def waitForPostgres =
    fs2.Stream
      .awakeDelay[IO](0.5.seconds)
      .evalMap { _ =>
        Session
          .single[IO](
            host = dbConfig.host,
            port = dbConfig.port,
            user = dbConfig.user.value,
            password = Some(dbConfig.pass.value),
            database = dbConfig.name.value
          )
          .use(_.unique(sql"SELECT 1".query(int4)))
          .attempt
      }
      .map(_.fold(_ => 1, _ => 0))
      .take(100)
      .find(_ == 0)
      .compile
      .drain

  def sessionPool(dbCfg: DBConfig[_]): Resource[IO, Resource[IO, Session[IO]]] =
    Session
      .pooled[IO](
        host = postgresContainer.host,
        port = dbConfig.port.value,
        database = dbCfg.name.value,
        user = dbCfg.user.value,
        password = Some(dbCfg.pass.value),
        max = dbConfig.connectionPool.value
      )

  def sessionPoolResource[A](dbCfg: DBConfig[_]): Resource[IO, SessionResource[IO, A]] =
    sessionPool(dbCfg).map(SessionResource[IO, A](_))

  def initializeDatabase(cfg: DBConfig[_]): IO[Unit] = {
    val session = Session.single[IO](
      host = dbConfig.host,
      port = dbConfig.port,
      user = dbConfig.user.value,
      password = Some(dbConfig.pass.value),
      database = dbConfig.name.value
    )

    // note: it would be simpler to use the default user that is created with the container, but that requires
    // to first refactor how the configuration is loaded. Currently services load it from the file every time and so
    // this creates the users as expected from the given config
    val createRole: Command[Void] =
      sql"create role #${cfg.user.value} with password '#${cfg.pass.value}' superuser login".command
    val createDatabase: Command[Void] = sql"create database  #${cfg.name.value}".command
    val grants:         Command[Void] = sql"grant all on database #${cfg.name.value} to #${cfg.user.value}".command

    session.use { s =>
      s.execute(createRole) *>
        s.execute(createDatabase) *>
        s.execute(grants)
    }.void
  }
}
