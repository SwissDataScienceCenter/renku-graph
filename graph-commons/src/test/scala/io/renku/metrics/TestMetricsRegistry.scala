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

package io.renku.metrics

import cats.MonadThrow
import io.prometheus.client.CollectorRegistry

object TestMetricsRegistry {
  def apply[F[_]: MonadThrow]: TestMetricsRegistry[F] = new TestMetricsRegistry[F]
}

class TestMetricsRegistry[F[_]: MonadThrow] extends MetricsRegistry[F] {

  private val collectorRegistry: CollectorRegistry = new CollectorRegistry()

  override def register[C <: MetricsCollector with PrometheusCollector](collector: C): F[C] =
    MonadThrow[F].catchNonFatal {
      collectorRegistry register collector.wrappedCollector
      collector
    }

  override def maybeCollectorRegistry: Option[CollectorRegistry] = Some(collectorRegistry)

  def clear(): Unit = collectorRegistry.clear()
}
