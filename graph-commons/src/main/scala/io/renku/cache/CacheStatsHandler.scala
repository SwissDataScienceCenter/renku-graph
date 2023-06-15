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

import cats.Monad
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.renku.cache.CacheStatsGauge.Label
import io.renku.metrics.MetricsRegistry

final class CacheStatsHandler[F[_]: Monad](gauge: CacheStatsGauge[F]) extends CacheEventHandler[F] {

  def widen: CacheEventHandler[F] = this

  override def apply(event: CacheEvent): F[Unit] = event match {
    case CacheEvent.CacheResponse(CacheResult.Hit(_, _), size) =>
      gauge.set(Label.CacheSize -> size) *> gauge.increment(Label.CacheHit)

    case CacheEvent.CacheResponse(CacheResult.Miss, size) =>
      gauge.set(Label.CacheSize -> size) *> gauge.increment(Label.CacheMiss)

    case CacheEvent.CacheClear(dropped, size) =>
      List(
        Label.CacheSize  -> size.toDouble,
        Label.CacheClear -> dropped.toDouble
      ).traverse_(gauge.set)
  }
}

object CacheStatsHandler {

  def apply[F[_]: Async: MetricsRegistry](cacheId: String Refined NonEmpty): F[CacheStatsHandler[F]] =
    CacheStatsGauge[F](cacheId).map(new CacheStatsHandler[F](_))
}
