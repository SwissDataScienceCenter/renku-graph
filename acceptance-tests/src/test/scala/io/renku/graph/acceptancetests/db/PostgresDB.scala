/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.db.{PostgresServer, SessionResource}
import natchez.Trace.Implicits.noop
import skunk._
import skunk.codec.all.bool
import skunk.implicits._

object PostgresDB {

  lazy val server: PostgresServer = new PostgresServer("acceptance_tests", port = 5432)

  private lazy val dbConfig = server.dbConfig

  def startUnsafe(): Unit = server.start()
  def forceStop():   Unit = server.forceStop()

  def start(): IO[Unit] =
    IO(startUnsafe())

  def sessionPool(dbCfg: DBConfig[_]): Resource[IO, Resource[IO, Session[IO]]] =
    Session
      .pooled[IO](
        host = dbCfg.host.value,
        port = dbConfig.port.value,
        database = dbCfg.name.value,
        user = dbCfg.user.value,
        password = Some(dbCfg.pass.value),
        max = dbConfig.connectionPool.value
      )

  def sessionPoolResource[A](dbCfg: DBConfig[A]): Resource[IO, SessionResource[IO, A]] =
    sessionPool(dbCfg).map(SessionResource[IO, A](_, dbCfg))

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
    val roleExists: Query[Void, Boolean] =
      sql"SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '#${cfg.user.value}')".query(bool)
    val createRole: Command[Void] =
      sql"create role #${cfg.user.value} with password '#${cfg.pass.value}' superuser login".command
    val createDatabase: Command[Void] = sql"create database  #${cfg.name.value}".command
    val grants:         Command[Void] = sql"grant all on database #${cfg.name.value} to #${cfg.user.value}".command

    session.use { s =>
      s.unique(roleExists) >>= {
        case true => ().pure[IO]
        case false =>
          s.execute(createRole) *>
            s.execute(createDatabase) *>
            s.execute(grants)
      }
    }.void
  }
}
