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

package io.renku.cache

import io.renku.cache.CacheConfig.ClearConfig

import scala.concurrent.duration._

/**
 *
 * @param evictStrategy defines the data used to decide when to evict entries
 * @param ignoreEmptyValues if `true` empty values are not cached
 * @param ttl the duration entries should live
 * @param clearConfig the cache is cleared to avoid growing too large
 */
final case class CacheConfig(
    evictStrategy:     EvictStrategy,
    ignoreEmptyValues: Boolean,
    ttl:               FiniteDuration,
    clearConfig:       ClearConfig
) {
  def isExpired(key: Key[_], currentTime: FiniteDuration) =
    evictStrategy.isExpired(key, ttl, currentTime)

  def isDisabled: Boolean =
    ttl <= Duration.Zero || clearConfig.maximumSize <= 0
}

object CacheConfig {

  val default: CacheConfig =
    CacheConfig(
      evictStrategy = EvictStrategy.Oldest,
      ignoreEmptyValues = true,
      ttl = 5.seconds,
      ClearConfig.default
    )

  sealed trait ClearConfig {
    def maximumSize: Int
  }
  object ClearConfig {
    final case class Periodic(maximumSize: Int, interval: FiniteDuration) extends ClearConfig

    val default: ClearConfig = Periodic(5000, 5.minutes)
  }
}
