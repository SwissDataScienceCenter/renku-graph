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

package ch.datascience.db

import cats.MonadError
import cats.syntax.all._
import ch.datascience.config.ConfigLoader
import ch.datascience.db.DBConfigProvider.DBConfig
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.W
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.pureconfig._
import eu.timepit.refined.string.MatchesRegex

import scala.concurrent.duration.FiniteDuration

class DBConfigProvider[Interpretation[_], TargetDB](
    namespace: String,
    dbName:    DBConfig.DbName,
    config:    Config = ConfigFactory.load()
)(implicit ME: MonadError[Interpretation, Throwable])
    extends ConfigLoader[Interpretation] {

  import DBConfigProvider._

  def map[Out](f: DBConfig[TargetDB] => Out): Interpretation[Out] =
    get() map f

  def get(): Interpretation[DBConfig[TargetDB]] =
    for {
      driver         <- find[DBConfig.Driver](s"$namespace.db-driver", config)
      host           <- find[DBConfig.Host](s"$namespace.db-host", config)
      user           <- find[DBConfig.User](s"$namespace.db-user", config)
      pass           <- find[DBConfig.Pass](s"$namespace.db-pass", config)
      connectionPool <- find[DBConfig.ConnectionPool](s"$namespace.connection-pool", config)
      maxLifetime    <- find[DBConfig.MaxLifetime](s"$namespace.max-connection-lifetime", config)
      urlTemplate    <- find[DBConfig.UrlTemplate](s"$namespace.db-url-template", config)
      url            <- findUrl(urlTemplate, host)
    } yield DBConfig(driver, url, user, pass, connectionPool, maxLifetime)

  private def findUrl(urlTeplate: DBConfig.UrlTemplate, host: DBConfig.Host): Interpretation[DBConfig.Url] =
    ME.fromEither {
      RefType
        .applyRef[DBConfig.Url](urlTeplate.value.replace("$host", host.value).replace("$dbName", dbName.value))
        .leftMap(_ => new IllegalArgumentException("Invalid db url value"))
    }
}

object DBConfigProvider {
  import DBConfig._

  case class DBConfig[TargetDB](driver:         Driver,
                                url:            Url,
                                user:           User,
                                pass:           Pass,
                                connectionPool: ConnectionPool,
                                maxLifetime:    MaxLifetime
  )
  object DBConfig {
    type Driver         = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
    type Url            = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
    type Host           = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
    type UrlTemplate    = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
    type DbName         = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
    type User           = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
    type Pass           = String
    type ConnectionPool = Int Refined Positive
    type MaxLifetime    = FiniteDuration
  }
}
