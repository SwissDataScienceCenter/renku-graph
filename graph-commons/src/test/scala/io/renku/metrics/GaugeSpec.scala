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
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.metrics.MetricsTools._
import org.scalacheck.{Arbitrary, Gen}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class SingleValueGaugeSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "set" should {

    "set the given value on the gauge" in new TestCase {

      val value = Arbitrary.arbDouble.arbitrary.generateOne

      gauge.set(value) shouldBe MonadThrow[Try].unit

      gauge.wrappedCollector.get() shouldBe value
    }
  }

  private trait TestCase {
    private val name = nonBlankStrings().generateOne
    private val help = sentences().generateOne

    val gauge = new SingleValueGaugeImpl[Try](name, help)
  }
}

class LabeledGaugeSpec extends AnyWordSpec with MockFactory with should.Matchers {
  import io.renku.graph.model.GraphModelGenerators._
  import io.renku.graph.model.projects.Path

  "set" should {

    "associate the given value with a label on the gauge" in new TestCase {

      // iteration 1
      val labelValue1 = projectPaths.generateOne
      val value1      = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue1 -> value1) shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain only ((label.value, labelValue1.value, value1))

      // iteration 2
      val labelValue2 = projectPaths.generateOne
      val value2      = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue2 -> value2) shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain.only(
        (label.value, labelValue1.value, value1),
        (label.value, labelValue2.value, value2)
      )
    }
  }

  "update" should {

    "update the value associated with the label - case without a label" in new TestCase {

      val labelValue = projectPaths.generateOne
      val update     = nonNegativeDoubles().generateOne.value
      gauge.update(labelValue -> update) shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain only ((label.value, labelValue.value, update))
    }

    "update the value associated with the label - case with a positive value" in new TestCase {

      val labelValue   = projectPaths.generateOne
      val initialValue = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue -> initialValue) shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain only ((label.value, labelValue.value, initialValue))

      val update = nonNegativeDoubles().generateOne.value
      gauge.update(labelValue -> update) shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain.only(
        (label.value, labelValue.value, initialValue + update)
      )
    }

    "update the value associated with the label - case with a negative value" in new TestCase {

      val labelValue   = projectPaths.generateOne
      val initialValue = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue -> initialValue) shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain only ((label.value, labelValue.value, initialValue))

      val update = initialValue * negativeInts(-5).generateOne
      gauge.update(labelValue -> -update) shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain.only(
        (label.value, labelValue.value, initialValue - update)
      )
    }
  }

  "reset" should {

    "replace all current entries with ones returned from the given reset data fetch function" in new TestCase {

      // before re-provisioning
      val labelValue1 = projectPaths.generateOne
      val value1      = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue1 -> value1) shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain only ((label.value, labelValue1.value, value1))

      // re-provisioning
      val waitingEvents = waitingEventsGen.generateNonEmptyList().toList.flatten.toMap
      resetDataFetch.expects().returning(waitingEvents.pure[Try])

      gauge.reset() shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain theSameElementsAs waitingEvents.map { case (labelValue, value) =>
        (label.value, labelValue.value, value)
      }
    }
  }

  "clear" should {

    "remove all entries" in new TestCase {

      // before re-provisioning
      val labelValue1 = projectPaths.generateOne
      val value1      = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue1 -> value1) shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain only ((label.value, labelValue1.value, value1))

      gauge.clear() shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples.isEmpty shouldBe true
    }
  }

  "increment" should {

    "increment value for the given label value" in new TestCase {

      // before re-provisioning
      val labelValue = projectPaths.generateOne
      val value      = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue -> value) shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain only ((label.value, labelValue.value, value))

      // incrementing
      gauge.increment(labelValue) shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain only ((label.value, labelValue.value, value + 1))
    }

    "add label value if one is not present yet" in new TestCase {

      val labelValue = projectPaths.generateOne

      gauge.increment(labelValue) shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain only ((label.value, labelValue.value, 1))
    }
  }

  "decrement" should {

    "decrement value for the given label value" in new TestCase {

      // before re-provisioning
      val labelValue = projectPaths.generateOne
      val value      = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue -> value) shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain only ((label.value, labelValue.value, value))

      // incrementing
      gauge.decrement(labelValue) shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain only ((label.value, labelValue.value, value - 1))
    }

    "add label value with value 0 if one is not present yet" in new TestCase {

      val labelValue = projectPaths.generateOne

      gauge.decrement(labelValue) shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples should contain only ((label.value, labelValue.value, 0))
    }
  }

  private trait TestCase {
    val label        = nonBlankStrings().generateOne
    private val name = nonBlankStrings().generateOne
    private val help = nonBlankStrings().generateOne

    val resetDataFetch = mockFunction[Try[Map[Path, Double]]]
    val gauge          = new LabeledGaugeImpl[Try, Path](name, help, label, resetDataFetch)
    val underlying     = gauge.wrappedCollector
  }

  private lazy val waitingEventsGen: Gen[Map[Path, Double]] = nonEmptySet {
    for {
      path  <- projectPaths
      count <- nonNegativeLongs()
    } yield path -> count.value.toDouble
  }.map(_.toMap)
}

class GaugeSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "apply without label names" should {

    "register the metrics in the Metrics Registry " +
      "and return an instance of the SingleValueGauge" in new TestCase {

        (metricsRegistry
          .register(_: MetricsCollector with PrometheusCollector))
          .expects(*)
          .onCall((c: MetricsCollector with PrometheusCollector) => c.pure[Try])

        val Success(gauge) = Gauge[Try](name, help)

        gauge.isInstanceOf[SingleValueGauge[Try]] shouldBe true
        gauge.name                                shouldBe name
        gauge.help                                shouldBe help
      }
  }

  "apply with a label name" should {

    "register the metrics in the Metrics Registry " +
      "and return an instance of the LabeledGauge" in new TestCase {

        (metricsRegistry
          .register(_: MetricsCollector with PrometheusCollector))
          .expects(*)
          .onCall((c: MetricsCollector with PrometheusCollector) => c.pure[Try])

        val labelName = nonBlankStrings().generateOne

        val Success(gauge) = Gauge[Try, String](name, help, labelName)

        gauge.isInstanceOf[LabeledGauge[Try, String]] shouldBe true
        gauge.name                                    shouldBe name
        gauge.help                                    shouldBe help
      }
  }

  private trait TestCase {
    val name = nonBlankStrings().generateOne
    val help = sentences().generateOne

    implicit val metricsRegistry: MetricsRegistry[Try] = mock[MetricsRegistry[Try]]
  }
}
