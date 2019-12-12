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

package ch.datascience.metrics

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.hotspot._
import pureconfig.loadConfig

import scala.language.higherKinds

object MetricsRegistry {

  private val metricsEnabled = loadConfig[Boolean]("metrics.enabled").getOrElse(true)

  private lazy val registry = addJvmMetrics(new CollectorRegistry())

  private[metrics] def collectorRegistry: CollectorRegistry =
    if (metricsEnabled) registry
    else new CollectorRegistry()

  private def addJvmMetrics(registry: CollectorRegistry): CollectorRegistry = {
    registry.register(new StandardExports())
    registry.register(new MemoryPoolsExports())
    registry.register(new BufferPoolsExports())
    registry.register(new GarbageCollectorExports())
    registry.register(new ThreadExports())
    registry.register(new ClassLoadingExports())
    registry.register(new VersionInfoExports())
    registry.register(new MemoryAllocationExports())
    registry
  }

  def clear(): Unit = collectorRegistry.clear()
}
