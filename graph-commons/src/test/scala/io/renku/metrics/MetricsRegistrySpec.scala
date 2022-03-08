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

import cats.effect.IO
import com.typesafe.config.ConfigFactory
import io.prometheus.client.{Gauge => LibGauge}
import io.renku.metrics.MetricsRegistry.{DisabledMetricsRegistry, EnabledMetricsRegistry}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.Try

class MetricsRegistrySpec extends AnyWordSpec with IOSpec with should.Matchers {

  "apply" should {

    "return a disabled Metrics Registry if the 'metrics.enabled' flag is set to false" in {
      val registry = MetricsRegistry[IO](
        ConfigFactory.parseMap(Map("metrics" -> Map("enabled" -> false).asJava).asJava)
      ).unsafeRunSync()

      registry.getClass shouldBe classOf[DisabledMetricsRegistry[Try]]
    }

    "return an enabled Metrics Registry if the 'metrics.enabled' flag is set to true" in {
      val registry = MetricsRegistry[IO](
        ConfigFactory.parseMap(Map("metrics" -> Map("enabled" -> true).asJava).asJava)
      ).unsafeRunSync()

      registry.getClass shouldBe classOf[EnabledMetricsRegistry[Try]]
    }

    "return an enabled Metrics Registry if there is no value for the 'metrics.enabled' flag" in {
      val registry = MetricsRegistry[IO](ConfigFactory.empty()).unsafeRunSync()
      registry.getClass shouldBe classOf[EnabledMetricsRegistry[Try]]
    }
  }

  "EnabledMetricsRegistry.register" should {

    "register the given collector in the collector registry" in {

      val registry = new EnabledMetricsRegistry[IO]

      val gaugeName = "gauge_name"
      val metricsCollector = new MetricsCollector with PrometheusCollector {
        override type Collector = LibGauge
        override val wrappedCollector: LibGauge = LibGauge
          .build()
          .name(gaugeName)
          .help("some gauge info")
          .labelNames("label")
          .create()
        override val name: String = wrappedCollector.describe().asScala.head.name
        override val help: String = wrappedCollector.describe().asScala.head.help
      }

      registry.register(metricsCollector).unsafeRunSync()

      registry.maybeCollectorRegistry.flatMap(
        _.metricFamilySamples().asScala
          .find(_.name == gaugeName)
          .map(_.samples.asScala.map(_.value).toList)
      ) shouldBe Some(List())
    }
  }

  "DisabledMetricsRegistry.register" should {

    "not register the given collector in any registry" in {

      val registry = new DisabledMetricsRegistry[Try]

      val gaugeName = "gauge_name"
      val metricsCollector = new MetricsCollector with PrometheusCollector {
        override type Collector = LibGauge
        override val wrappedCollector: LibGauge = LibGauge
          .build()
          .name(gaugeName)
          .help("some gauge info")
          .labelNames("label")
          .create()
        override val name: String = wrappedCollector.describe().asScala.head.name
        override val help: String = wrappedCollector.describe().asScala.head.help
      }

      registry register metricsCollector

      registry.maybeCollectorRegistry shouldBe None
    }
  }
}
