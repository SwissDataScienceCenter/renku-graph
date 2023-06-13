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

import scala.concurrent.duration.FiniteDuration

sealed trait EvictStrategy {
  private[cache] def keyOrder[A]: Ordering[Key[A]]
  private[cache] def isExpired(key: Key[_], ttl: FiniteDuration, currentTime: FiniteDuration): Boolean
}

object EvictStrategy {
  case object LeastUsed extends EvictStrategy {
    private[cache] override def keyOrder[A]: Ordering[Key[A]] = Key.Order.leastRecentlyUsed

    private[cache] def isExpired(key: Key[_], ttl: FiniteDuration, currentTime: FiniteDuration): Boolean =
      (key.accessedAt + ttl.toSeconds) < currentTime.toSeconds
  }

  case object Oldest extends EvictStrategy {
    private[cache] override def keyOrder[A]: Ordering[Key[A]] = Key.Order.oldest

    private[cache] def isExpired(key: Key[_], ttl: FiniteDuration, currentTime: FiniteDuration): Boolean =
      (key.createdAt + ttl.toSeconds) < currentTime.toSeconds
  }
}
