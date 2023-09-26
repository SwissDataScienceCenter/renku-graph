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
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.util.{Success, Try}

class SingleValueHistogramSpec extends AnyWordSpec with MockFactory with should.Matchers with TryValues {

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

  "observe(Double)" should {

    "call the underlying impl" in new TestCase {
      val histogram = new SingleValueHistogramImpl[Try](name, help, Seq(0.1).some)
      histogram.observe(101d)
      histogram.collectAllSamples.map(toValue) shouldBe List(0d, 1d)
    }
  }

  "observe(FiniteDuration)" should {

    "convert the duration to seconds in Double format" in new TestCase {
      val histogram = new SingleValueHistogramImpl[Try](name, help, Seq(0.1).some)
      histogram.observe(101 millis)
      histogram.collectAllSamples.map(toValue) shouldBe List(0d, 1d)
    }
  }

  "observe(Option[String], FiniteDuration)" should {

    "update the underlying histogram in case of no label given" in {

      val histogram = new SingleValueHistogramImpl[Try](nonBlankStrings().generateOne,
                                                        nonBlankStrings().generateOne,
                                                        maybeBuckets = Seq(0.1d).some
      )

      histogram.observe(maybeLabel = None, 101 millis).isSuccess shouldBe true

      histogram.collectAllSamples.map(toValue) shouldBe List(0d, 1d)
    }

    "log an error if some label given" in {

      val histogram = new SingleValueHistogramImpl[Try](nonBlankStrings().generateOne,
                                                        nonBlankStrings().generateOne,
                                                        maybeBuckets = Seq(0.1d).some
      )

      val label     = nonEmptyStrings().generateOne
      val exception = histogram.observe(label.some, 101 millis).failure.exception

      exception.getMessage shouldBe s"Label $label sent for a Single Value Histogram ${histogram.name}"
    }
  }

  private trait TestCase {
    val name = nonBlankStrings().generateOne
    val help = sentences().generateOne
  }
}

class LabeledHistogramSpec extends AnyWordSpec with MockFactory with should.Matchers with TryValues {

  "apply" should {

    "register the metrics in the Metrics Registry " +
      "and return an instance of the LabeledHistogram" in new TestCase {

        implicit val metricsRegistry: MetricsRegistry[Try] = mock[MetricsRegistry[Try]]

        (metricsRegistry
          .register(_: MetricsCollector with PrometheusCollector))
          .expects(*)
          .onCall((c: MetricsCollector with PrometheusCollector) => c.pure[Try])

        val labelName = nonBlankStrings().generateOne

        val Success(histogram) = Histogram[Try](name, help, labelName, Seq(.1, 1), maybeThreshold = None)

        histogram.isInstanceOf[LabeledHistogram[Try]] shouldBe true
        histogram.name                                shouldBe name
        histogram.help                                shouldBe help
      }
  }

  "observe(Double)" should {

    "store the value if no threshold given" in new TestCase {

      val histogram = new LabeledHistogramImpl[Try](name, help, "label", Seq(0.1).some)

      histogram.observe("label", 1.001d)

      histogram.wrappedCollector.labels("label").get().sum shouldBe 1.001d
    }

    "store the value if threshold given but the value >= the threshold" in new TestCase {

      val histogram =
        new LabeledHistogramImpl[Try](name, help, "label", Seq(0.1).some, maybeThreshold = (1 second).some)

      histogram.observe("label", 1.001d)

      histogram.wrappedCollector.labels("label").get().sum shouldBe 1.001d
    }

    "not store the value if threshold given and the value < the threshold" in new TestCase {

      val histogram =
        new LabeledHistogramImpl[Try](name, help, "label", Seq(0.1).some, maybeThreshold = (1 second).some)

      histogram.observe("label", .999d)

      histogram.wrappedCollector.labels("label").get().sum shouldBe 0d
    }
  }

  "observe(FiniteDuration)" should {

    "store the value if no threshold given" in new TestCase {

      val histogram = new LabeledHistogramImpl[Try](name, help, "label", Seq(0.1).some)

      histogram.observe("label", 1001 millis)

      histogram.wrappedCollector.labels("label").get().sum shouldBe 1.001d
    }

    "store the value if threshold given but the value >= the threshold" in new TestCase {

      val histogram =
        new LabeledHistogramImpl[Try](name, help, "label", Seq(0.1).some, maybeThreshold = (1 second).some)

      histogram.observe("label", 1001 millis)

      histogram.wrappedCollector.labels("label").get().sum shouldBe 1.001d
    }

    "not store the value if threshold given and the value < the threshold" in new TestCase {

      val histogram =
        new LabeledHistogramImpl[Try](name, help, "label", Seq(0.1).some, maybeThreshold = (1 second).some)

      histogram.observe("label", 999 millis)

      histogram.wrappedCollector.labels("label").get().sum shouldBe 0d
    }
  }

  "observe(Option[String], FiniteDuration)" should {

    "update the underlying histogram for the given label" in {

      val histogram = new LabeledHistogramImpl[Try](nonBlankStrings().generateOne,
                                                    nonBlankStrings().generateOne,
                                                    nonBlankStrings().generateOne,
                                                    maybeBuckets = Seq(0.1d).some
      )

      val label    = nonEmptyStrings().generateOne
      val duration = 101 millis

      histogram.observe(label.some, duration).success.value shouldBe ()

      histogram.sumValuesFor(label) shouldBe duration.toMillis.toDouble / 1000
    }

    "do nothing in case no label given" in {

      val histogram = new LabeledHistogramImpl[Try](nonBlankStrings().generateOne,
                                                    nonBlankStrings().generateOne,
                                                    nonBlankStrings().generateOne,
                                                    maybeBuckets = Seq(0.1d).some
      )

      histogram.observe(None, durations().generateOne).success.value shouldBe ()

      histogram.collectAllSamples shouldBe Seq.empty
    }
  }

  private trait TestCase {
    val name = nonBlankStrings().generateOne
    val help = sentences().generateOne
  }
}
