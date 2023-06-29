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

package io.renku.metrics

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.metrics.MetricsTools._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.lang.Thread.sleep
import scala.concurrent.duration._
import scala.util.{Success, Try}

class SingleValueHistogramSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "apply" should {

    "register the metrics in the Metrics Registry " +
      "and return an instance of the LabeledHistogram" in new TestCase {

        implicit val metricsRegistry: MetricsRegistry[Try] = mock[MetricsRegistry[Try]]

        (metricsRegistry
          .register(_: MetricsCollector with PrometheusCollector))
          .expects(*)
          .onCall((c: MetricsCollector with PrometheusCollector) => c.pure[Try])

        val Success(histogram) = Histogram[Try](name, help, Seq(.1, 1))

        histogram.isInstanceOf[SingleValueHistogram[Try]] shouldBe true
        histogram.name                                    shouldBe name
        histogram.help                                    shouldBe help
      }
  }

  "startTimer -> observeDuration" should {

    "collect measured duration" in new TestCase {

      val histogram  = new SingleValueHistogramImpl[Try](name, help, Seq(0.1))
      val underlying = histogram.wrappedCollector

      val simulatedDuration = 500 millis
      val observedDuration  = histogram.simulateDuration(simulatedDuration)

      (observedDuration * 1000)              should be > simulatedDuration.toMillis.toDouble
      underlying.collectAllSamples.last._3 shouldBe 1d
    }
  }

  "observe" should {
    "call the underlying impl" in new TestCase {
      val histogram = new LabeledHistogramImpl[Try](name, help, "label", Seq(0.1))
      histogram.observe("label", 101d)
      histogram.wrappedCollector.labels("label").get().sum shouldBe 101d
    }
  }

  private trait TestCase {
    val name = nonBlankStrings().generateOne
    val help = sentences().generateOne
  }

  private implicit class HistogramOps(histogram: SingleValueHistogram[Try]) {

    def simulateDuration(duration: Duration): Double = {
      val Success(timer) = histogram.startTimer()

      sleep(duration.toMillis)

      timer.observeDuration.fold(throw _, identity)
    }
  }
}

class LabeledHistogramSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "apply" should {

    "register the metrics in the Metrics Registry " +
      "and return an instance of the LabeledHistogram" in new TestCase {

        implicit val metricsRegistry: MetricsRegistry[Try] = mock[MetricsRegistry[Try]]

        (metricsRegistry
          .register(_: MetricsCollector with PrometheusCollector))
          .expects(*)
          .onCall((c: MetricsCollector with PrometheusCollector) => c.pure[Try])

        val labelName = nonBlankStrings().generateOne

        val Success(histogram) = Histogram[Try](name, help, labelName, Seq(.1, 1))

        histogram.isInstanceOf[LabeledHistogram[Try]] shouldBe true
        histogram.name                                shouldBe name
        histogram.help                                shouldBe help
      }
  }

  "startTimer -> observeDuration" should {

    "collect measured duration for the label" in new TestCase {

      val histogram  = new LabeledHistogramImpl[Try](name, help, label, Seq(0.1))
      val underlying = histogram.wrappedCollector

      val value             = nonEmptyStrings().generateOne
      val simulatedDuration = 500 millis
      val observedDuration  = histogram.simulateDuration(simulatedDuration, value)

      (observedDuration * 1000)                       should be > simulatedDuration.toMillis.toDouble
      underlying.collectValuesFor(label.value, value) should contain(observedDuration)
    }

    "collect measured time only for durations longer than threshold" in new TestCase {

      val threshold  = 500 millis
      val histogram  = new LabeledHistogramImpl[Try](name, help, label, Seq(0.1), maybeThreshold = threshold.some)
      val underlying = histogram.wrappedCollector

      // process below threshold
      val value1                  = nonEmptyStrings().generateOne
      val value1SimulatedDuration = threshold minus (200 millis)

      val value1ObservedDuration = histogram.simulateDuration(value1SimulatedDuration, value1)

      (value1ObservedDuration * 1000) should (
        (be > value1SimulatedDuration.toMillis.toDouble) and (be < threshold.toMillis.toDouble)
      )
      underlying.collectValuesFor(label.value, value1) shouldBe Nil

      // process above threshold
      val value2                  = nonEmptyStrings().generateOne
      val value2SimulatedDuration = threshold plus (200 millis)

      val value2ObservedDuration = histogram.simulateDuration(value2SimulatedDuration, value2)

      underlying.collectValuesFor(label.value, value2) should contain(value2ObservedDuration)

      // another process below threshold
      val value3                  = nonEmptyStrings().generateOne
      val value3SimulatedDuration = threshold minus (200 millis)

      histogram.simulateDuration(value3SimulatedDuration, value3)

      underlying.collectValuesFor(label.value, value1) shouldBe Nil
    }

    "remove the collected label value once an observed duration for it gets below the threshold" in new TestCase {

      val threshold  = 500 millis
      val histogram  = new LabeledHistogramImpl[Try](name, help, label, Seq(0.1), maybeThreshold = threshold.some)
      val underlying = histogram.wrappedCollector
      val value      = nonEmptyStrings().generateOne

      // process below threshold
      val simulatedDuration1 = threshold minus (200 millis)

      histogram.simulateDuration(simulatedDuration1, value)

      underlying.collectValuesFor(label.value, value) shouldBe Nil

      // process above threshold
      val simulatedDuration2 = threshold plus (200 millis)

      val observedDuration2 = histogram.simulateDuration(simulatedDuration2, value)

      underlying.collectValuesFor(label.value, value) should contain(observedDuration2)

      // another process below threshold
      val simulatedDuration3 = threshold minus (200 millis)

      histogram.simulateDuration(simulatedDuration3, value)

      underlying.collectValuesFor(label.value, value) shouldBe Nil
    }
  }

  private trait TestCase {
    val label = nonBlankStrings().generateOne
    val name  = nonBlankStrings().generateOne
    val help  = sentences().generateOne
  }

  private implicit class HistogramOps(histogram: LabeledHistogram[Try]) {

    def simulateDuration(duration: Duration, value: String): Double = {
      val Success(timer) = histogram.startTimer(value)

      sleep(duration.toMillis)

      timer.observeDuration.fold(throw _, identity)
    }
  }
}
