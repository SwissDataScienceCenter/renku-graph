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

package ch.datascience.triplesgenerator.config

import cats.MonadError
import cats.implicits._
import ch.datascience.config.ConfigLoader
import ch.datascience.http.client.{BasicAuthCredentials, BasicAuthPassword, BasicAuthUsername}
import ch.datascience.tinytypes.constraints.{NonBlank, Url, UrlOps}
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

import scala.language.higherKinds

class DatasetName private (val value: String) extends AnyVal with TinyType[String]

object DatasetName extends TinyTypeFactory[String, DatasetName](new DatasetName(_)) with NonBlank {

  private[config] implicit val datasetNameReader: ConfigReader[DatasetName] =
    ConfigReader.fromString[DatasetName] { value =>
      DatasetName
        .from(value)
        .leftMap(exception => CannotConvert(value, DatasetName.getClass.toString, exception.getMessage))
    }
}

sealed trait DatasetType extends TinyType[String] with Product with Serializable

object DatasetType {

  final case object Mem extends DatasetType {
    override val value: String = "mem"
  }

  final case object TDB extends DatasetType {
    override val value: String = "tdb"
  }

  private[config] implicit val datasetTypeReader: ConfigReader[DatasetType] =
    ConfigReader.fromString[DatasetType] {
      case Mem.value => Right(Mem)
      case TDB.value => Right(TDB)
      case other =>
        Left(CannotConvert(other, DatasetType.getClass.toString, s"$other is neither $Mem nor $TDB"))
    }
}

final case class FusekiConfig(
    fusekiBaseUrl:   FusekiBaseUrl,
    datasetName:     DatasetName,
    datasetType:     DatasetType,
    authCredentials: BasicAuthCredentials
)

class FusekiBaseUrl private (val value: String) extends AnyVal with TinyType[String]
object FusekiBaseUrl
    extends TinyTypeFactory[String, FusekiBaseUrl](new FusekiBaseUrl(_))
    with Url
    with UrlOps[FusekiBaseUrl] {
  private[config] implicit val fusekiBaseUrlReader: ConfigReader[FusekiBaseUrl] =
    ConfigReader.fromString[FusekiBaseUrl] { value =>
      FusekiBaseUrl
        .from(value)
        .leftMap(exception => CannotConvert(value, FusekiBaseUrl.getClass.toString, exception.getMessage))
    }
}

class FusekiConfigProvider[Interpretation[_]](
    config:    Config = ConfigFactory.load()
)(implicit ME: MonadError[Interpretation, Throwable])
    extends ConfigLoader[Interpretation] {

  import ch.datascience.http.client.BasicAuthConfigReaders._

  def get: Interpretation[FusekiConfig] =
    for {
      url         <- find[FusekiBaseUrl]("services.fuseki.url", config)
      datasetName <- find[DatasetName]("services.fuseki.dataset-name", config)
      datasetType <- find[DatasetType]("services.fuseki.dataset-type", config)
      username    <- find[BasicAuthUsername]("services.fuseki.username", config)
      password    <- find[BasicAuthPassword]("services.fuseki.password", config)
    } yield FusekiConfig(url, datasetName, datasetType, BasicAuthCredentials(username, password))
}
