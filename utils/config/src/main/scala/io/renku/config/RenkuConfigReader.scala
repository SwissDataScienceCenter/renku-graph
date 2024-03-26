/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import cats.syntax.all._
import io.renku.control.RateLimit
import io.renku.http.client.GitLabUrl
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

import scala.util.Try

trait RenkuConfigReader {

  implicit def rateLimitReader[T]: ConfigReader[RateLimit[T]] =
    ConfigReader.fromString[RateLimit[T]] { value =>
      RateLimit
        .from[Try, T](value)
        .toEither
        .leftMap(exception => CannotConvert(value, RateLimit.getClass.toString, exception.getMessage))
    }

  implicit val serviceVersionReader: ConfigReader[ServiceVersion] =
    ConfigLoader.stringTinyTypeReader(ServiceVersion)

  implicit val serviceNameReader: ConfigReader[ServiceName] =
    ConfigLoader.stringTinyTypeReader(ServiceName)

  implicit val gitLabUrlReader: ConfigReader[GitLabUrl] =
    ConfigLoader.urlTinyTypeReader(GitLabUrl)
}

object RenkuConfigReader extends RenkuConfigReader
