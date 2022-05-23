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

package io.renku.config

import cats.MonadThrow
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.graph.model.views.TinyTypeJsonLDOps
import io.renku.tinytypes.constraints.NonBlank
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}
import pureconfig.ConfigReader

final class ServiceName private (val value: String) extends AnyVal with StringTinyType
object ServiceName extends TinyTypeFactory[ServiceName](new ServiceName(_)) with NonBlank[ServiceName] {
  import ConfigLoader._
  implicit val reader: ConfigReader[ServiceName] = stringTinyTypeReader(ServiceName)

  def readFromConfig[F[_]: MonadThrow](versionConfig: Config = ConfigFactory.load()): F[ServiceName] =
    find[F, ServiceName]("service-name", versionConfig)
}

final class ServiceVersion private (val value: String) extends AnyVal with StringTinyType
object ServiceVersion
    extends TinyTypeFactory[ServiceVersion](new ServiceVersion(_))
    with NonBlank[ServiceVersion]
    with TinyTypeJsonLDOps[ServiceVersion] {

  import ConfigLoader._
  import io.circe.Decoder
  import io.renku.tinytypes.json.TinyTypeDecoders.stringDecoder

  implicit val reader:  ConfigReader[ServiceVersion] = stringTinyTypeReader(ServiceVersion)
  implicit val decoder: Decoder[ServiceVersion]      = stringDecoder(ServiceVersion)

  def readFromConfig[F[_]: MonadThrow](versionConfig: Config = ConfigFactory.load("version.conf")): F[ServiceVersion] =
    find[F, ServiceVersion]("version", versionConfig)
}
