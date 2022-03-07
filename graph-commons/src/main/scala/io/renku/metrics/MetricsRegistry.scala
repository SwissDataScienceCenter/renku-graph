/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import io.prometheus.client.hotspot._
import io.prometheus.client.{CollectorRegistry, SimpleCollector}

import scala.util.control.NonFatal

trait MetricsRegistry[F[_]] {

  def register[Collector <: SimpleCollector[_], Builder <: SimpleCollector.Builder[Builder, Collector]](
      collectorBuilder: Builder
  ): F[Collector]

  def maybeCollectorRegistry: Option[CollectorRegistry]
}

object MetricsRegistry {

  import cats.syntax.all._
  import com.typesafe.config.{Config, ConfigFactory}
  import io.renku.config.ConfigLoader.find

  def apply[F[_]: MonadThrow](implicit mr: MetricsRegistry[F]): MetricsRegistry[F] = mr

  def apply[F[_]: MonadThrow](config: Config = ConfigFactory.load()): F[MetricsRegistry[F]] = for {
    maybeEnabled <- find[F, Option[Boolean]]("metrics.enabled", config) recoverWith noneValue
  } yield maybeEnabled match {
    case Some(false) => new DisabledMetricsRegistry[F]
    case _           => new EnabledMetricsRegistry[F]
  }

  private def noneValue[F[_]: MonadThrow]: PartialFunction[Throwable, F[Option[Boolean]]] = { case NonFatal(_) =>
    Option(true).pure[F]
  }

  class DisabledMetricsRegistry[F[_]: MonadThrow] extends MetricsRegistry[F] {

    override def register[Collector <: SimpleCollector[_], Builder <: SimpleCollector.Builder[Builder, Collector]](
        collectorBuilder: Builder
    ): F[Collector] = MonadThrow[F].catchNonFatal(collectorBuilder.create())

    override lazy val maybeCollectorRegistry: Option[CollectorRegistry] = None
  }

  class EnabledMetricsRegistry[F[_]: MonadThrow] extends MetricsRegistry[F] {

    import EnabledMetricsRegistry._

    override def register[Collector <: SimpleCollector[_], Builder <: SimpleCollector.Builder[Builder, Collector]](
        collectorBuilder: Builder
    ): F[Collector] = MonadThrow[F].catchNonFatal {
      collectorBuilder register registry
    }

    override lazy val maybeCollectorRegistry: Option[CollectorRegistry] = Some(registry)
  }

  object EnabledMetricsRegistry {
    private lazy val registry: CollectorRegistry = addJvmMetrics(new CollectorRegistry())

    private def addJvmMetrics(registry: CollectorRegistry): CollectorRegistry = {
      registry register new StandardExports()
      registry register new MemoryPoolsExports()
      registry register new BufferPoolsExports()
      registry register new GarbageCollectorExports()
      registry register new ThreadExports()
      registry register new ClassLoadingExports()
      registry register new VersionInfoExports()
      registry register new MemoryAllocationExports()
      registry
    }
  }
}
