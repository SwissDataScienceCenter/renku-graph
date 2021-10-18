/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import cats.MonadError
import io.prometheus.client.hotspot._
import io.prometheus.client.{CollectorRegistry, SimpleCollector}

import scala.util.control.NonFatal

trait MetricsRegistry[Interpretation[_]] {

  def register[Collector <: SimpleCollector[_], Builder <: SimpleCollector.Builder[Builder, Collector]](
      collectorBuilder: Builder
  )(implicit ME:        MonadError[Interpretation, Throwable]): Interpretation[Collector]

  def maybeCollectorRegistry: Option[CollectorRegistry]
}

object MetricsRegistry {

  import cats.effect.IO
  import cats.syntax.all._
  import com.typesafe.config.{Config, ConfigFactory}
  import io.renku.config.ConfigLoader.find

  def apply(
      config: Config = ConfigFactory.load()
  ): IO[MetricsRegistry[IO]] =
    for {
      maybeEnabled <- find[IO, Option[Boolean]]("metrics.enabled", config) recoverWith noneValue
    } yield maybeEnabled match {
      case Some(false) => DisabledMetricsRegistry
      case _           => EnabledMetricsRegistry
    }

  private val noneValue: PartialFunction[Throwable, IO[Option[Boolean]]] = { case NonFatal(_) =>
    IO.pure(Some(true))
  }

  object DisabledMetricsRegistry extends MetricsRegistry[IO] {

    override def register[Collector <: SimpleCollector[_], Builder <: SimpleCollector.Builder[Builder, Collector]](
        collectorBuilder: Builder
    )(implicit ME:        MonadError[IO, Throwable]): IO[Collector] = IO {
      collectorBuilder.create()
    }

    override lazy val maybeCollectorRegistry: Option[CollectorRegistry] = None
  }

  object EnabledMetricsRegistry extends MetricsRegistry[IO] {

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

    override def register[Collector <: SimpleCollector[_], Builder <: SimpleCollector.Builder[Builder, Collector]](
        collectorBuilder: Builder
    )(implicit ME:        MonadError[IO, Throwable]): IO[Collector] = IO {
      collectorBuilder register registry
    }

    override lazy val maybeCollectorRegistry: Option[CollectorRegistry] = Some(registry)
  }
}
