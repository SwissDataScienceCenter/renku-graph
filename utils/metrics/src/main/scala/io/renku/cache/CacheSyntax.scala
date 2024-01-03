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

import cats.effect.Async
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.renku.metrics.MetricsRegistry

trait CacheSyntax {

  final implicit class CacheBuilderExt[F[_], A, B](self: CacheBuilder[F, A, B]) {
    def withCacheStats(
        cacheId: String Refined NonEmpty
    )(implicit R: MetricsRegistry[F], async: Async[F]): CacheBuilder[F, A, B] =
      CacheMetricsBuilder(self).withCacheStats(cacheId)
  }
}
