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

package ch.datascience.config

import cats.implicits._
import ch.datascience.tinytypes.constraints.{Url, UrlOps}
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

import scala.language.implicitConversions
import scala.util.Try

class ServiceUrl private (val value: String) extends AnyVal with TinyType[String]

object ServiceUrl extends TinyTypeFactory[String, ServiceUrl](new ServiceUrl(_)) with Url with UrlOps[ServiceUrl] {

  implicit val serviceUrlReader: ConfigReader[ServiceUrl] =
    ConfigReader.fromString[ServiceUrl] { value =>
      Try(ServiceUrl(value)).toEither
        .leftMap(exception => CannotConvert(value, ServiceUrl.getClass.toString, exception.getMessage))
    }
}
