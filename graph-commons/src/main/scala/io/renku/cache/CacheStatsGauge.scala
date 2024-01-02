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

import cats.effect.kernel.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.metrics.{LabeledGauge, MetricsRegistry, PositiveValuesLabeledGauge}
import eu.timepit.refined.collection.NonEmpty

trait CacheStatsGauge[F[_]] extends LabeledGauge[F, CacheStatsGauge.Label]

object CacheStatsGauge {
  sealed trait Label extends Product {
    final lazy val asString: String = camelToSnake(productPrefix)
    override def toString = asString
  }
  object Label {
    case object CacheHit   extends Label
    case object CacheMiss  extends Label
    case object CacheClear extends Label
    case object CacheSize  extends Label
  }

  def apply[F[_]: Async: MetricsRegistry](cacheId: String Refined NonEmpty): F[CacheStatsGauge[F]] =
    MetricsRegistry[F]
      .register(
        new PositiveValuesLabeledGauge[F, CacheStatsGauge.Label](
          name = cacheId,
          help = s"Statistics for cache",
          labelName = "cache_stats",
          resetDataFetch = () => Async[F].pure(Map.empty)
        ) with CacheStatsGauge[F]
      )
      .widen

  private def camelToSnake(str: String): String =
    if (str.isEmpty) str
    else if (str.tail.isEmpty) str.toLowerCase
    else {
      val first = str.head.toLower
      val rest  = str.tail.flatMap(c => if (c.isUpper) s"_${c.toLower}" else c.toString)
      s"$first$rest"
    }
}
