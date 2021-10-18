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

package io.renku.rdfstore

import cats.MonadThrow
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.ConfigLoader.{stringTinyTypeReader, urlTinyTypeReader}
import io.renku.http.client.{BasicAuthCredentials, BasicAuthPassword, BasicAuthUsername}
import io.renku.tinytypes.constraints.{NonBlank, Url, UrlOps}
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory, UrlTinyType}
import pureconfig.ConfigReader

final case class RdfStoreConfig(
    fusekiBaseUrl:   FusekiBaseUrl,
    datasetName:     DatasetName,
    authCredentials: BasicAuthCredentials
)

object RdfStoreConfig {

  import io.renku.config.ConfigLoader._
  import io.renku.http.client.BasicAuthConfigReaders._

  def apply[Interpretation[_]: MonadThrow](
      config: Config = ConfigFactory.load()
  ): Interpretation[RdfStoreConfig] =
    for {
      url         <- find[Interpretation, FusekiBaseUrl]("services.fuseki.url", config)
      datasetName <- find[Interpretation, DatasetName]("services.fuseki.dataset-name", config)
      username    <- find[Interpretation, BasicAuthUsername]("services.fuseki.renku.username", config)
      password    <- find[Interpretation, BasicAuthPassword]("services.fuseki.renku.password", config)
    } yield RdfStoreConfig(url, datasetName, BasicAuthCredentials(username, password))
}

class FusekiBaseUrl private (val value: String) extends AnyVal with UrlTinyType

object FusekiBaseUrl extends TinyTypeFactory[FusekiBaseUrl](new FusekiBaseUrl(_)) with Url with UrlOps[FusekiBaseUrl] {
  implicit val fusekiBaseUrlReader: ConfigReader[FusekiBaseUrl] = urlTinyTypeReader(FusekiBaseUrl)
}

class DatasetName private (val value: String) extends AnyVal with StringTinyType

object DatasetName extends TinyTypeFactory[DatasetName](new DatasetName(_)) with NonBlank {
  implicit val datasetNameReader: ConfigReader[DatasetName] = stringTinyTypeReader(DatasetName)
}
