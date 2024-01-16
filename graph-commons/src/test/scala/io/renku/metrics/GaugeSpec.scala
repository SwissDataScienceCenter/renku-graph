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

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class GaugeSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {

  "apply without label names" should {

    "register the metrics in the Metrics Registry " +
      "and return an instance of the SingleValueGauge" in new TestCase {

        (metricsRegistry
          .register(_: MetricsCollector with PrometheusCollector))
          .expects(*)
          .onCall((c: MetricsCollector with PrometheusCollector) => c.pure[IO])

        val gauge = Gauge[IO](name, help).unsafeRunSync()

        gauge.isInstanceOf[SingleValueGauge[IO]] shouldBe true
        gauge.name                               shouldBe name
        gauge.help                               shouldBe help
      }
  }

  "apply with a label name" should {

    "register the metrics in the Metrics Registry " +
      "and return an instance of the LabeledGauge" in new TestCase {

        (metricsRegistry
          .register(_: MetricsCollector with PrometheusCollector))
          .expects(*)
          .onCall((c: MetricsCollector with PrometheusCollector) => c.pure[IO])

        val labelName = nonBlankStrings().generateOne

        val gauge = Gauge[IO, String](name, help, labelName).unsafeRunSync()

        gauge.isInstanceOf[LabeledGauge[IO, String]] shouldBe true
        gauge.name                                   shouldBe name
        gauge.help                                   shouldBe help
      }
  }

  private trait TestCase {
    val name = nonBlankStrings().generateOne
    val help = sentences().generateOne

    implicit val metricsRegistry: MetricsRegistry[IO] = mock[MetricsRegistry[IO]]
  }
}
