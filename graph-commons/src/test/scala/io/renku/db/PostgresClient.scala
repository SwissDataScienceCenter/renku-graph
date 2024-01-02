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

package io.renku.db

import cats.effect._
import cats.effect.std.Random
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import io.renku.db.DBConfigProvider.DBConfig
import natchez.Trace.Implicits.noop
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import skunk.{Session, SqlState}
import skunk.implicits._

class PostgresClient[DB](server: PostgresServer, migrations: SessionResource[IO, DB] => IO[Unit]) {

  private[this] implicit val logger: Logger[IO] = Slf4jLogger.getLoggerFromClass[IO](getClass)

  def randomizedDBResource(prefix: String): Resource[IO, DBConfig[DB]] = {
    Random
      .scalaUtilRandom[IO]
      .flatMap(_.nextIntBounded(1000))
      .map(v => s"${prefix}_$v")
      .toResource >>= dbResource
  }.recoverWith { case SqlState.DuplicateDatabase(_) => randomizedDBResource(prefix) }

  def dbResource(dbName: String): Resource[IO, DBConfig[DB]] =
    Resource
      .make(
        logger.debug(s"Creating test database: $dbName") *>
          initSession
            .use(_.execute(sql"""CREATE DATABASE "#$dbName" OWNER #${server.dbConfig.user.value}""".command).void)
            .as(server.dbConfig.copy(name = Refined.unsafeApply(dbName)).asInstanceOf[DBConfig[DB]])
      )(_ =>
        logger.debug(s"Dropping test database $dbName") *>
          initSession
            .use(_.execute(sql"""DROP DATABASE "#$dbName"""".command).void)
            .void
      )
      .evalTap(cfg => migrations(SessionResource[IO, DB](sessionResource(cfg), cfg)))

  private lazy val initSession: Resource[IO, Session[IO]] = makeSession(server.dbConfig)

  def sessionResource(cfg: DBConfig[DB]): Resource[IO, Session[IO]] =
    makeSession(cfg)

  private def makeSession(cfg: DBConfig[_]): Resource[IO, Session[IO]] =
    Session.single[IO](
      host = cfg.host.value,
      port = cfg.port.value,
      user = cfg.user.value,
      password = cfg.pass.value.some,
      database = cfg.name.value
    )
}
