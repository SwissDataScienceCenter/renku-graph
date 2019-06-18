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

package ch.datascience.graphservice.rdfstore

import RDFStoreConfig._
import cats.MonadError
import cats.implicits._
import ch.datascience.config.ConfigLoader
import ch.datascience.tinytypes.constraints.{NonBlank, Url, UrlOps}
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

import scala.language.higherKinds

final case class RDFStoreConfig(
    fusekiBaseUrl: FusekiBaseUrl,
    datasetName:   DatasetName
)

object RDFStoreConfig {

  import ConfigLoader._

  def apply[Interpretation[_]](
      config:    Config = ConfigFactory.load()
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[RDFStoreConfig] =
    for {
      url         <- find[Interpretation, FusekiBaseUrl]("services.fuseki.url", config)
      datasetName <- find[Interpretation, DatasetName]("services.fuseki.dataset-name", config)
    } yield RDFStoreConfig(url, datasetName)

  final class FusekiBaseUrl private (val value: String) extends AnyVal with TinyType[String]
  object FusekiBaseUrl
      extends TinyTypeFactory[String, FusekiBaseUrl](new FusekiBaseUrl(_))
      with Url
      with UrlOps[FusekiBaseUrl]

  final class DatasetName private (val value: String) extends AnyVal with TinyType[String]
  object DatasetName extends TinyTypeFactory[String, DatasetName](new DatasetName(_)) with NonBlank

  private implicit val fusekiBaseUrlReader: ConfigReader[FusekiBaseUrl] =
    ConfigReader.fromString[FusekiBaseUrl] { value =>
      FusekiBaseUrl
        .from(value)
        .leftMap(exception => CannotConvert(value, FusekiBaseUrl.getClass.toString, exception.getMessage))
    }

  private implicit val datasetNameReader: ConfigReader[DatasetName] =
    ConfigReader.fromString[DatasetName] { value =>
      DatasetName
        .from(value)
        .leftMap(exception => CannotConvert(value, DatasetName.getClass.toString, exception.getMessage))
    }
}
