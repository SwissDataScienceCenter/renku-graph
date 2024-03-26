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

package io.renku.cache

import cats.MonadThrow
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.ConfigLoader
import pureconfig.error.{CannotConvert, ConfigReaderFailures}
import pureconfig.generic.auto._
import pureconfig.{ConfigReader, ConfigSource}

object CacheConfigLoader {

  implicit val evictStrategyReader: ConfigReader[EvictStrategy] =
    ConfigReader.fromString {
      case "least-recently-used" => Right(EvictStrategy.LeastRecentlyUsed)
      case "oldest"              => Right(EvictStrategy.Oldest)
      case str                   => Left(CannotConvert(str, "EvictStrategy", "Unknown value"))
    }

  def read(config: Config): Either[ConfigReaderFailures, CacheConfig] =
    ConfigSource.fromConfig(config).load[CacheConfig]

  def unsafeRead(config: Config): CacheConfig =
    ConfigSource.fromConfig(config).loadOrThrow[CacheConfig]

  def load[F[_]: MonadThrow](at: String, config: Config = ConfigFactory.load()) =
    ConfigLoader.find[F, CacheConfig](at, config)
}
