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

package io.renku.rdfstore

import cats.MonadThrow
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.ConfigLoader.urlTinyTypeReader
import io.renku.http.client.{BasicAuthCredentials, BasicAuthPassword, BasicAuthUsername}
import io.renku.tinytypes.constraints.{Url, UrlOps}
import io.renku.tinytypes.{TinyTypeFactory, UrlTinyType}
import pureconfig.ConfigReader

trait TriplesStoreConfig {
  val fusekiUrl:       FusekiUrl
  val datasetName:     DatasetName
  val authCredentials: BasicAuthCredentials
}

final case class RdfStoreConfig(fusekiUrl: FusekiUrl, authCredentials: BasicAuthCredentials)
    extends TriplesStoreConfig {
  val datasetName: DatasetName = RdfStoreConfig.RenkuDS
}

object RdfStoreConfig {

  val RenkuDS: DatasetName = DatasetName("renku")

  import io.renku.config.ConfigLoader._
  import io.renku.http.client.BasicAuthConfigReaders._

  def apply[F[_]: MonadThrow](config: Config = ConfigFactory.load()): F[RdfStoreConfig] = for {
    url      <- find[F, FusekiUrl]("services.fuseki.url", config)
    username <- find[F, BasicAuthUsername]("services.fuseki.renku.username", config)
    password <- find[F, BasicAuthPassword]("services.fuseki.renku.password", config)
  } yield RdfStoreConfig(url, BasicAuthCredentials(username, password))
}

final case class MigrationsStoreConfig(
    fusekiUrl:       FusekiUrl,
    authCredentials: BasicAuthCredentials
) extends TriplesStoreConfig {
  val datasetName: DatasetName = MigrationsStoreConfig.MigrationsDS
}

object MigrationsStoreConfig {

  val MigrationsDS: DatasetName = DatasetName("migrations")

  import io.renku.config.ConfigLoader._
  import io.renku.http.client.BasicAuthConfigReaders._

  def apply[F[_]: MonadThrow](config: Config = ConfigFactory.load()): F[MigrationsStoreConfig] = for {
    url      <- find[F, FusekiUrl]("services.fuseki.url", config)
    username <- find[F, BasicAuthUsername]("services.fuseki.admin.username", config)
    password <- find[F, BasicAuthPassword]("services.fuseki.admin.password", config)
  } yield MigrationsStoreConfig(url, BasicAuthCredentials(username, password))
}

class FusekiUrl private (val value: String) extends AnyVal with UrlTinyType
object FusekiUrl extends TinyTypeFactory[FusekiUrl](new FusekiUrl(_)) with Url[FusekiUrl] with UrlOps[FusekiUrl] {
  implicit val fusekiUrlReader: ConfigReader[FusekiUrl] = urlTinyTypeReader(FusekiUrl)
}
