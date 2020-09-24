/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import cats.MonadError
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.metrics.MetricsTools._
import io.prometheus.client.{Gauge => LibGauge}
import org.scalacheck.{Arbitrary, Gen}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class SingleValueGaugeSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "set" should {

    "set the given value on the gauge" in new TestCase {

      val value = Arbitrary.arbDouble.arbitrary.generateOne

      gauge.set(value) shouldBe ME.unit

      underlying.get() shouldBe value
    }
  }

  private trait TestCase {
    val ME = MonadError[Try, Throwable]

    private val name = nonBlankStrings().generateOne
    private val help = sentences().generateOne
    val underlying   = LibGauge.build(name.value, help.value).create()

    val gauge = new SingleValueGaugeImpl[Try](underlying)
  }
}

class LabeledGaugeSpec extends AnyWordSpec with MockFactory with should.Matchers {
  import ch.datascience.graph.model.GraphModelGenerators._
  import ch.datascience.graph.model.projects.Path

  "set" should {

    "associate the given value with a label on the gauge" in new TestCase {

      // iteration 1
      val labelValue1 = projectPaths.generateOne
      val value1      = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue1 -> value1) shouldBe ME.unit

      underlying.collectAllSamples should contain only ((label, labelValue1.value, value1))

      // iteration 2
      val labelValue2 = projectPaths.generateOne
      val value2      = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue2 -> value2) shouldBe ME.unit

      underlying.collectAllSamples should contain.only(
        (label, labelValue1.value, value1),
        (label, labelValue2.value, value2)
      )
    }
  }

  "reset" should {

    "replace all current entries with ones returned from the given reset data fetch function" in new TestCase {

      // before re-provisioning
      val labelValue1 = projectPaths.generateOne
      val value1      = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue1 -> value1) shouldBe ME.unit

      underlying.collectAllSamples should contain only ((label, labelValue1.value, value1))

      // re-provisioning
      val waitingEvents = waitingEventsGen.generateNonEmptyList().toList.flatten.toMap
      resetDataFetch.expects().returning(waitingEvents.pure[Try])

      gauge.reset() shouldBe ME.unit

      underlying.collectAllSamples should contain theSameElementsAs waitingEvents.map { case (labelValue, value) =>
        (label, labelValue.value, value)
      }
    }
  }

  "increment" should {

    "increment value for the given label value" in new TestCase {

      // before re-provisioning
      val labelValue = projectPaths.generateOne
      val value      = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue -> value) shouldBe ME.unit

      underlying.collectAllSamples should contain only ((label, labelValue.value, value))

      // incrementing
      gauge.increment(labelValue) shouldBe ME.unit

      underlying.collectAllSamples should contain only ((label, labelValue.value, value + 1))
    }

    "add label value if one is not present yet" in new TestCase {

      val labelValue = projectPaths.generateOne

      gauge.increment(labelValue) shouldBe ME.unit

      underlying.collectAllSamples should contain only ((label, labelValue.value, 1))
    }
  }

  "decrement" should {

    "decrement value for the given label value" in new TestCase {

      // before re-provisioning
      val labelValue = projectPaths.generateOne
      val value      = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue -> value) shouldBe ME.unit

      underlying.collectAllSamples should contain only ((label, labelValue.value, value))

      // incrementing
      gauge.decrement(labelValue) shouldBe ME.unit

      underlying.collectAllSamples should contain only ((label, labelValue.value, value - 1))
    }

    "add label value with value 0 if one is not present yet" in new TestCase {

      val labelValue = projectPaths.generateOne

      gauge.decrement(labelValue) shouldBe ME.unit

      underlying.collectAllSamples should contain only ((label, labelValue.value, 0))
    }
  }

  private trait TestCase {
    val ME = MonadError[Try, Throwable]

    val label        = nonBlankStrings().generateOne.value
    private val name = nonBlankStrings().generateOne
    private val help = sentences().generateOne
    val underlying   = LibGauge.build(name.value, help.value).labelNames(label).create()

    val resetDataFetch = mockFunction[Try[Map[Path, Double]]]
    val gauge          = new LabeledGaugeImpl[Try, Path](underlying, resetDataFetch)
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
          .register[LibGauge, LibGauge.Builder](_: LibGauge.Builder)(_: MonadError[Try, Throwable]))
          .expects(*, *)
          .onCall { (builder: LibGauge.Builder, _: MonadError[Try, Throwable]) =>
            builder.create().pure[Try]
          }

        val Success(gauge) = Gauge[Try](name, help)(metricsRegistry)

        gauge.isInstanceOf[SingleValueGauge[Try]] shouldBe true
        gauge.name                                shouldBe name.value
        gauge.help                                shouldBe help.value
      }
  }

  "apply with a label name" should {

    "register the metrics in the Metrics Registry " +
      "and return an instance of the LabeledGauge" in new TestCase {

        (metricsRegistry
          .register[LibGauge, LibGauge.Builder](_: LibGauge.Builder)(_: MonadError[Try, Throwable]))
          .expects(*, *)
          .onCall { (builder: LibGauge.Builder, _: MonadError[Try, Throwable]) =>
            builder.create().pure[Try]
          }

        val labelName = nonBlankStrings().generateOne

        val Success(gauge) = Gauge[Try, String](name, help, labelName)(metricsRegistry)

        gauge.isInstanceOf[LabeledGauge[Try, String]] shouldBe true
        gauge.name                                    shouldBe name.value
        gauge.help                                    shouldBe help.value
      }
  }

  private trait TestCase {
    val ME = MonadError[Try, Throwable]

    val name = nonBlankStrings().generateOne
    val help = sentences().generateOne

    val metricsRegistry = mock[MetricsRegistry[Try]]
  }
}
