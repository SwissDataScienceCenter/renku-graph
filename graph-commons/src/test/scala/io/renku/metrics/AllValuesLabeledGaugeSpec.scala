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

import MetricsTools._
import cats.MonadThrow
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{ints, negativeDoubles, nonBlankStrings, nonEmptySet, nonNegativeDoubles, nonNegativeLongs}
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class AllValuesLabeledGaugeSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {
  import io.renku.graph.model.GraphModelGenerators.projectPaths
  import io.renku.graph.model.projects.Path

  "set" should {

    "associate the given value with a label on the gauge" in new TestCase {

      // iteration 1
      val labelValue1 = projectPaths.generateOne
      val value1      = nonNegativeDoubles().generateOne.value

      gauge.set(labelValue1 -> value1) shouldBe MonadThrow[Try].unit

      underlying.collectValuesFor(label.value, labelValue1.value) shouldBe List(value1)

      // iteration 2
      val labelValue2 = projectPaths.generateOne
      val value2      = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue2 -> value2) shouldBe MonadThrow[Try].unit

      underlying.collectValuesFor(label.value, labelValue2.value) shouldBe List(value2)
    }

    "set negative value if one is set" in new TestCase {

      val labelValue = projectPaths.generateOne
      val value      = negativeDoubles().generateOne.value

      gauge.set(labelValue -> value) shouldBe MonadThrow[Try].unit

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(value)
    }
  }

  "update" should {

    "update the value associated with the label - case without a label and positive update" in new TestCase {

      val labelValue = projectPaths.generateOne
      val update     = nonNegativeDoubles().generateOne.value
      gauge.update(labelValue -> update) shouldBe MonadThrow[Try].unit

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(update)
    }

    "update the value associated with the label - case without a label and negative update" in new TestCase {

      val labelValue = projectPaths.generateOne
      val update     = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue -> -update) shouldBe MonadThrow[Try].unit

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(-update)
    }

    "update the value associated with the label - case with a positive update value" in new TestCase {

      val labelValue   = projectPaths.generateOne
      val initialValue = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue -> initialValue) shouldBe MonadThrow[Try].unit

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(initialValue)

      val update = nonNegativeDoubles().generateOne.value
      gauge.update(labelValue -> update) shouldBe MonadThrow[Try].unit

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(initialValue + update)
    }

    "update the value associated with the label - case with a negative update value so current + update < 0" in new TestCase {

      val labelValue   = projectPaths.generateOne
      val initialValue = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue -> initialValue) shouldBe MonadThrow[Try].unit

      val update = initialValue * ints(min = 2, max = 5).generateOne
      gauge.update(labelValue -> -update) shouldBe MonadThrow[Try].unit

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(initialValue - update)
    }
  }

  "reset" should {

    "replace all current entries with ones returned from the given reset data fetch function" in new TestCase {

      // before re-provisioning
      val labelValue1 = projectPaths.generateOne
      val value1      = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue1 -> value1) shouldBe MonadThrow[Try].unit

      underlying.collectValuesFor(label.value, labelValue1.value) shouldBe List(value1)

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

      val labelValue = projectPaths.generateOne
      val value      = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue -> value) shouldBe MonadThrow[Try].unit

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(value)

      gauge.clear() shouldBe MonadThrow[Try].unit

      underlying.collectAllSamples.isEmpty shouldBe true
    }
  }

  "increment" should {

    "increment value for the given label value" in new TestCase {

      val labelValue = projectPaths.generateOne
      val value      = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue -> value) shouldBe MonadThrow[Try].unit

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(value)

      // incrementing
      gauge.increment(labelValue) shouldBe MonadThrow[Try].unit

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(value + 1)
    }

    "add label value if one is not present yet" in new TestCase {

      val labelValue = projectPaths.generateOne

      gauge.increment(labelValue) shouldBe MonadThrow[Try].unit

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(1)
    }
  }

  "decrement" should {

    "decrement value for the given label value" in new TestCase {

      val labelValue = projectPaths.generateOne
      val value      = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue -> value) shouldBe MonadThrow[Try].unit

      // decrementing
      gauge.decrement(labelValue) shouldBe MonadThrow[Try].unit

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(value - 1)
    }

    "add label value with value -1 if one is not present yet" in new TestCase {

      val labelValue = projectPaths.generateOne

      gauge.decrement(labelValue) shouldBe MonadThrow[Try].unit

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(-1)
    }
  }

  private trait TestCase {
    val label          = nonBlankStrings().generateOne
    val name           = nonBlankStrings().generateOne
    val help           = nonBlankStrings().generateOne
    val resetDataFetch = mockFunction[Try[Map[Path, Double]]]

    val gauge      = new AllValuesLabeledGauge[Try, Path](name, help, label, resetDataFetch)
    val underlying = gauge.wrappedCollector
  }

  private lazy val waitingEventsGen: Gen[Map[Path, Double]] = nonEmptySet {
    for {
      path  <- projectPaths
      count <- nonNegativeLongs()
    } yield path -> count.value.toDouble
  }.map(_.toMap)
}
