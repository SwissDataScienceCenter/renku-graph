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

package ch.datascience.db

import cats.MonadError
import cats.syntax.all._
import ch.datascience.config.ConfigLoader
import ch.datascience.db.DBConfigProvider.DBConfig
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.W
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.pureconfig._
import eu.timepit.refined.string.MatchesRegex

import scala.concurrent.duration.FiniteDuration

class DBConfigProvider[Interpretation[_]: MonadError[*[_], Throwable], TargetDB](
    namespace: String,
    dbName:    DBConfig.DbName,
    config:    Config = ConfigFactory.load()
) extends ConfigLoader[Interpretation] {

  import DBConfigProvider._

  def map[Out](f: DBConfig[TargetDB] => Out): Interpretation[Out] =
    get() map f

  def get(): Interpretation[DBConfig[TargetDB]] =
    for {
      host           <- find[DBConfig.Host](s"$namespace.db-host", config)
      port           <- find[DBConfig.Port](s"$namespace.db-port", config)
      user           <- find[DBConfig.User](s"$namespace.db-user", config)
      pass           <- find[DBConfig.Pass](s"$namespace.db-pass", config)
      connectionPool <- find[DBConfig.ConnectionPool](s"$namespace.connection-pool", config)
      maxLifetime    <- find[DBConfig.MaxLifetime](s"$namespace.max-connection-lifetime", config)
    } yield DBConfig(host, port, dbName, user, pass, connectionPool, maxLifetime)
}

object DBConfigProvider {

  import DBConfig._

  case class DBConfig[TargetDB](
      host:           Host,
      port:           Port,
      name:           DbName,
      user:           User,
      pass:           Pass,
      connectionPool: ConnectionPool,
      maxLifetime:    MaxLifetime
  )

  object DBConfig {
    type Host           = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
    type Port           = Int Refined Positive
    type DbName         = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
    type User           = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
    type Pass           = String Refined NonEmpty
    type ConnectionPool = Int Refined Positive
    type MaxLifetime    = FiniteDuration
  }
}
