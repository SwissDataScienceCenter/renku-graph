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
import cats.syntax.all._
import io.prometheus.client.{Histogram => LibHistogram}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.metrics.MetricsTools._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.lang.Thread.sleep
import scala.util.{Success, Try}

class HistogramSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "Labeled Histogram" should {

    "has apply method " +
      "that registers the metrics in the Metrics Registry " +
      "and return an instance of the LabeledHistogram" in new TestCase {

        (metricsRegistry
          .register(_: MetricsCollector with PrometheusCollector))
          .expects(*)
          .onCall((c: MetricsCollector with PrometheusCollector) => c.pure[Try])

        val labelName = nonBlankStrings().generateOne

        val Success(histogram) = Histogram[Try](name, help, labelName, Seq(.1, 1))

        histogram.isInstanceOf[LabeledHistogram[Try]] shouldBe true
        histogram.name                                shouldBe name.value
        histogram.help                                shouldBe help.value
      }

    "has startTimer returning a timer measuring the elapsed time" in new TestCase {
      (metricsRegistry
        .register(_: MetricsCollector with PrometheusCollector))
        .expects(*)
        .onCall((c: MetricsCollector with PrometheusCollector) => c.pure[Try])

      val labelName          = nonBlankStrings().generateOne
      val Success(histogram) = Histogram[Try](name, help, labelName, Seq(.1, 1))

      val labelValue = nonEmptyStrings().generateOne

      val Success(timer) = histogram.startTimer(labelValue)

      sleep(500)

      val Success(duration) = timer.observeDuration

      duration should (be > .5d)

      histogram
        .asInstanceOf[LabeledHistogramImpl[IO]]
        .wrappedCollector
        .collectAllSamples should contain((labelName.value, labelValue, duration))
    }
  }

  "Single Value Histogram" should {

    "has apply method " +
      "that registers the metrics in the Metrics Registry " +
      "and return an instance of the SingleValueHistogram" in new TestCase {

        (metricsRegistry
          .register(_: MetricsCollector with PrometheusCollector))
          .expects(*)
          .onCall((c: MetricsCollector with PrometheusCollector) => c.pure[Try])

        val Success(histogram) = Histogram[Try](name, help, Seq(.1, 1))

        histogram.isInstanceOf[SingleValueHistogram[Try]] shouldBe true
        histogram.name                                    shouldBe name.value
        histogram.help                                    shouldBe help.value
      }

    "has startTimer returning a timer measuring the elapsed time" in new TestCase {
      (metricsRegistry
        .register(_: MetricsCollector with PrometheusCollector))
        .expects(*)
        .onCall((c: MetricsCollector with PrometheusCollector) => c.pure[Try])

      val Success(histogram) = Histogram[Try](name, help, Seq(.1, .5, 1))

      val labelValue = nonEmptyStrings().generateOne

      val Success(timer) = histogram.startTimer()

      sleep(600)

      val Success(duration) = timer.observeDuration

      duration should (be > .5d)

      histogram
        .asInstanceOf[SingleValueHistogramImpl[IO]]
        .wrappedCollector
        .collectAllSamples should contain(("le", "1.0", 1d))
    }
  }

  private trait TestCase {
    val name = nonBlankStrings().generateOne
    val help = sentences().generateOne

    implicit val metricsRegistry: MetricsRegistry[Try] = mock[MetricsRegistry[Try]]
  }
}

class LabeledHistogramSpec extends AnyWordSpec with MockFactory with should.Matchers {

  import MetricsTools._
  import io.renku.graph.model.GraphModelGenerators._

  "startTimer -> observeDuration" should {

    "associate measured time with the label on the histogram" in new TestCase {

      // iteration 1
      val labelValue1    = projectPaths.generateOne.value
      val Success(timer) = histogram.startTimer(labelValue1)

      val sleepTime = 500
      sleep(sleepTime)

      val Success(duration) = timer.observeDuration

      (duration * 1000) should be > sleepTime.toDouble

      underlying.collectAllSamples.map(_._1)             should contain(label)
      underlying.collectAllSamples.map(_._2)             should contain(labelValue1)
      underlying.collectAllSamples.map(_._3).last * 1000 should be > sleepTime.toDouble
    }
  }

  private trait TestCase {
    val label        = nonBlankStrings().generateOne.value
    private val name = nonBlankStrings().generateOne
    private val help = sentences().generateOne
    val underlying   = LibHistogram.build(name.value, help.value).labelNames(label).create()

    val histogram = new LabeledHistogramImpl[Try](underlying)
  }
}
