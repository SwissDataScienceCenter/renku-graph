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

package io.renku.triplesstore

import cats.MonadThrow
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.ConfigLoader.urlTinyTypeReader
import io.renku.http.client.{BasicAuthCredentials, BasicAuthPassword, BasicAuthUsername}
import io.renku.tinytypes.constraints.{Url, UrlOps}
import io.renku.tinytypes.{TinyTypeFactory, UrlTinyType}
import io.renku.triplesstore.client.http.{ConnectionConfig, Retry}
import org.http4s.{BasicCredentials, Uri}
import pureconfig.ConfigReader

trait FusekiConnectionConfig {
  val fusekiUrl:       FusekiUrl
  val authCredentials: BasicAuthCredentials
}

final case class AdminConnectionConfig(fusekiUrl: FusekiUrl, authCredentials: BasicAuthCredentials)
    extends FusekiConnectionConfig

object AdminConnectionConfig {
  import io.renku.config.ConfigLoader._
  import io.renku.http.client.BasicAuthConfigReaders._

  def apply[F[_]: MonadThrow](config: Config = ConfigFactory.load()): F[AdminConnectionConfig] = for {
    url      <- find[F, FusekiUrl]("services.fuseki.url", config)
    username <- find[F, BasicAuthUsername]("services.fuseki.admin.username", config)
    password <- find[F, BasicAuthPassword]("services.fuseki.admin.password", config)
  } yield AdminConnectionConfig(url, BasicAuthCredentials(username, password))
}

trait DatasetConnectionConfig extends FusekiConnectionConfig {
  val fusekiUrl:       FusekiUrl
  val datasetName:     DatasetName
  val authCredentials: BasicAuthCredentials

  def toCC(retryCfg: Option[Retry.RetryConfig] = None): ConnectionConfig =
    ConnectionConfig(
      Uri.unsafeFromString(fusekiUrl.value) / datasetName.value,
      Some(BasicCredentials(authCredentials.username.value, authCredentials.password.value)),
      retryCfg
    )
}

final case class ProjectsConnectionConfig(fusekiUrl: FusekiUrl, authCredentials: BasicAuthCredentials)
    extends DatasetConnectionConfig {
  val datasetName: DatasetName = ProjectsConnectionConfig.ProjectsDS
}

object ProjectsConnectionConfig {

  val ProjectsDS: DatasetName = DatasetName("projects")

  import io.renku.config.ConfigLoader._
  import io.renku.http.client.BasicAuthConfigReaders._

  def apply[F[_]: MonadThrow](config: Config = ConfigFactory.load()): F[ProjectsConnectionConfig] = for {
    url      <- find[F, FusekiUrl]("services.fuseki.url", config)
    username <- find[F, BasicAuthUsername]("services.fuseki.renku.username", config)
    password <- find[F, BasicAuthPassword]("services.fuseki.renku.password", config)
  } yield ProjectsConnectionConfig(url, BasicAuthCredentials(username, password))
}

final case class MigrationsConnectionConfig(
    fusekiUrl:       FusekiUrl,
    authCredentials: BasicAuthCredentials
) extends DatasetConnectionConfig {
  val datasetName: DatasetName = MigrationsConnectionConfig.MigrationsDS
}

object MigrationsConnectionConfig {

  val MigrationsDS: DatasetName = DatasetName("migrations")

  import io.renku.config.ConfigLoader._
  import io.renku.http.client.BasicAuthConfigReaders._

  def apply[F[_]: MonadThrow](config: Config = ConfigFactory.load()): F[MigrationsConnectionConfig] = for {
    url      <- find[F, FusekiUrl]("services.fuseki.url", config)
    username <- find[F, BasicAuthUsername]("services.fuseki.admin.username", config)
    password <- find[F, BasicAuthPassword]("services.fuseki.admin.password", config)
  } yield MigrationsConnectionConfig(url, BasicAuthCredentials(username, password))
}

class FusekiUrl private (val value: String) extends AnyVal with UrlTinyType
object FusekiUrl extends TinyTypeFactory[FusekiUrl](new FusekiUrl(_)) with Url[FusekiUrl] with UrlOps[FusekiUrl] {
  implicit val fusekiUrlReader: ConfigReader[FusekiUrl] = urlTinyTypeReader(FusekiUrl)
}
