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

package io.renku.control

import cats.MonadThrow
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.ConfigLoader

import scala.util.Try

object RateLimitLoader {

  def fromConfig[F[_]: MonadThrow, Target](
      key:    String,
      config: Config = ConfigFactory.load()
  ): F[RateLimit[Target]] = {
    import ConfigLoader._
    import pureconfig.ConfigReader
    import pureconfig.error.CannotConvert

    implicit val rateLimitReader: ConfigReader[RateLimit[Target]] =
      ConfigReader.fromString[RateLimit[Target]] { value =>
        RateLimit
          .from[Try, Target](value)
          .toEither
          .leftMap(exception => CannotConvert(value, RateLimit.getClass.toString, exception.getMessage))
      }

    ConfigLoader.find[F, RateLimit[Target]](key, config)
  }
}
