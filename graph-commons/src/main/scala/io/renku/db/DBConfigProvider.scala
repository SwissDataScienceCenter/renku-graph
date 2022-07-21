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

package io.renku.db

import cats.MonadThrow
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.pureconfig._
import io.renku.config.ConfigLoader
import io.renku.db.DBConfigProvider.DBConfig

class DBConfigProvider[F[_]: MonadThrow, TargetDB](
    namespace: String,
    dbName:    DBConfig.DbName,
    config:    Config = ConfigFactory.load()
) extends ConfigLoader[F] {

  import DBConfigProvider._

  def map[Out](f: DBConfig[TargetDB] => Out): F[Out] = get() map f

  def get(): F[DBConfig[TargetDB]] = for {
    host           <- find[DBConfig.Host](s"$namespace.db-host", config)
    port           <- find[DBConfig.Port](s"$namespace.db-port", config)
    user           <- find[DBConfig.User](s"$namespace.db-user", config)
    pass           <- find[DBConfig.Pass](s"$namespace.db-pass", config)
    connectionPool <- find[DBConfig.ConnectionPool](s"$namespace.connection-pool", config)
  } yield DBConfig(host, port, dbName, user, pass, connectionPool)
}

object DBConfigProvider {

  import DBConfig._

  case class DBConfig[TargetDB](
      host:           Host,
      port:           Port,
      name:           DbName,
      user:           User,
      pass:           Pass,
      connectionPool: ConnectionPool
  )

  object DBConfig {
    type Host           = String Refined NonEmpty
    type Port           = Int Refined Positive
    type DbName         = String Refined NonEmpty
    type User           = String Refined NonEmpty
    type Pass           = String Refined NonEmpty
    type ConnectionPool = Int Refined Positive
  }
}
