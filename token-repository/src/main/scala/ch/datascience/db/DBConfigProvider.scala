/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import eu.timepit.refined.pureconfig._
import cats.MonadError
import cats.implicits._
import ch.datascience.config.ConfigLoader
import ch.datascience.db.DBConfigProvider.DBConfig
import ch.datascience.orchestration.Provider
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.W
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.string.MatchesRegex

import scala.language.higherKinds

class DBConfigProvider[Interpretation[_]](
    namespace: String,
    driver:    DBConfig.Driver,
    dbName:    DBConfig.DbName,
    urlPrefix: DBConfig.UrlPrefix,
    config:    Config = ConfigFactory.load()
)(implicit ME: MonadError[Interpretation, Throwable])
    extends ConfigLoader[Interpretation]
    with Provider[Interpretation, DBConfig] {

  import DBConfigProvider._

  override def get(): Interpretation[DBConfig] =
    for {
      host <- find[DBConfig.Host](s"$namespace.db-host", config)
      user <- find[DBConfig.User](s"$namespace.db-user", config)
      pass <- find[DBConfig.Pass](s"$namespace.db-pass", config)
      url  <- findUrl(host)
    } yield DBConfig(driver, url, user, pass)

  private def findUrl(host: DBConfig.Host): Interpretation[DBConfig.Url] = ME.fromEither {
    RefType
      .applyRef[DBConfig.Url](s"$urlPrefix://$host/$dbName")
      .leftMap(_ => new IllegalArgumentException("Invalid db url value"))
  }
}

object DBConfigProvider {
  import DBConfig._

  case class DBConfig(driver: Driver, url: Url, user: User, pass: Pass)
  object DBConfig {
    type Driver    = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
    type Url       = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
    type Host      = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
    type UrlPrefix = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
    type DbName    = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
    type User      = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
    type Pass      = String
  }
}
