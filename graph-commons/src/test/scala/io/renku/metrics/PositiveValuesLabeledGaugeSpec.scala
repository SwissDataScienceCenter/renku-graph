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
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{ints, negativeDoubles, nonBlankStrings, nonEmptySet, nonNegativeDoubles, nonNegativeLongs}
import io.renku.graph.model.GraphModelGenerators.projectSlugs
import io.renku.graph.model.projects.Slug
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.lang.Thread.sleep
import scala.concurrent.duration._

class PositiveValuesLabeledGaugeSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {

  "set" should {

    "associate the given value with a label on the gauge" in new TestCase {

      // iteration 1
      val labelValue1 = projectSlugs.generateOne
      val value1      = nonNegativeDoubles().generateOne.value

      gauge.set(labelValue1 -> value1).unsafeRunSync() shouldBe ()

      underlying.collectValuesFor(label.value, labelValue1.value) shouldBe List(value1)

      // iteration 2
      val labelValue2 = projectSlugs.generateOne
      val value2      = nonNegativeDoubles().generateOne.value

      gauge.set(labelValue2 -> value2).unsafeRunSync() shouldBe ()

      underlying.collectValuesFor(label.value, labelValue2.value) shouldBe List(value2)
    }

    "set 0 if negatives value is given" in new TestCase {

      val labelValue = projectSlugs.generateOne
      val value      = negativeDoubles().generateOne.value

      gauge.set(labelValue -> value).unsafeRunSync() shouldBe ()

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(0d)
    }
  }

  "update" should {

    "update the value associated with the label - case without a label and positive update" in new TestCase {

      val labelValue = projectSlugs.generateOne
      val update     = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue -> update).unsafeRunSync() shouldBe ()

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(update)
    }

    "update the value associated with the label - case without a label and negative update" in new TestCase {

      val labelValue = projectSlugs.generateOne
      val update     = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue -> -update).unsafeRunSync() shouldBe ()

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(0)
    }

    "update the value associated with the label - case with a positive update value" in new TestCase {

      val labelValue   = projectSlugs.generateOne
      val initialValue = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue -> initialValue).unsafeRunSync() shouldBe ()

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(initialValue)

      val update = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue -> update).unsafeRunSync() shouldBe ()

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(initialValue + update)
    }

    "update the value associated with the label - case with a negative update value so current + update < 0" in new TestCase {

      val labelValue   = projectSlugs.generateOne
      val initialValue = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue -> initialValue).unsafeRunSync() shouldBe ()

      val update = initialValue * ints(min = 2, max = 5).generateOne

      gauge.update(labelValue -> -update).unsafeRunSync() shouldBe ()

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(0d)
    }
  }

  "reset" should {

    "replace all current entries with ones returned from the given reset data fetch function" in new TestCase {

      val labelValue1 = projectSlugs.generateOne
      val value1      = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue1 -> value1).unsafeRunSync() shouldBe ()

      underlying.collectValuesFor(label.value, labelValue1.value) shouldBe List(value1)

      // re-provisioning
      val waitingEvents = waitingEventsGen.generateNonEmptyList().toList.flatten.toMap
      resetDataFetch.expects().returning(waitingEvents.pure[IO])

      gauge.reset().unsafeRunSync() shouldBe ()

      underlying.collectAllSamples should contain theSameElementsAs waitingEvents.map { case (labelValue, value) =>
        (label.value, labelValue.value, value)
      }
    }
  }

  "clear" should {

    "remove all entries" in new TestCase {

      val labelValue1 = projectSlugs.generateOne
      val value1      = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue1 -> value1).unsafeRunSync() shouldBe ()

      underlying.collectValuesFor(label.value, labelValue1.value) shouldBe List(value1)

      gauge.clear().unsafeRunSync() shouldBe ()

      underlying.collectAllSamples.isEmpty shouldBe true
    }
  }

  "increment" should {

    "increment value for the given label value" in new TestCase {

      val labelValue = projectSlugs.generateOne
      val value      = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue -> value).unsafeRunSync() shouldBe ()

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(value)

      // incrementing
      gauge.increment(labelValue).unsafeRunSync() shouldBe ()

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(value + 1)
    }

    "add label value if one is not present yet" in new TestCase {

      val labelValue = projectSlugs.generateOne

      gauge.increment(labelValue).unsafeRunSync() shouldBe ()

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(1)
    }
  }

  "decrement" should {

    "decrement value for the given label value" in new TestCase {

      val labelValue = projectSlugs.generateOne
      val value      = nonNegativeDoubles().generateOne.value

      gauge.update(labelValue -> value).unsafeRunSync() shouldBe ()

      // decrementing
      gauge.decrement(labelValue).unsafeRunSync() shouldBe ()

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(value - 1)
    }

    "add label value with value 0 if one is not present yet" in new TestCase {

      val labelValue = projectSlugs.generateOne

      gauge.decrement(labelValue).unsafeRunSync() shouldBe ()

      underlying.collectValuesFor(label.value, labelValue.value) shouldBe List(0)
    }
  }

  "removing zeroed labels" should {

    "remove the label if 0 value was set to it" in new TestCase {

      gauge.startZeroedValuesCleaning(zeroRemovalTimeout).unsafeRunSync()

      // setting two positive value labels
      val labelValue1 = projectSlugs.generateOne
      val value1      = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue1 -> value1).unsafeRunSync() shouldBe ()

      val labelValue2 = projectSlugs.generateOne
      val value2      = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue2 -> value2).unsafeRunSync() shouldBe ()

      gauge.wrappedCollector.collectValuesFor(label.value, labelValue1.value) shouldBe List(value1)
      gauge.wrappedCollector.collectValuesFor(label.value, labelValue2.value) shouldBe List(value2)

      // one label gets 0
      gauge.set(labelValue2 -> 0d).unsafeRunSync() shouldBe ()

      sleep(zeroRemovalTimeout.toMillis - (zerosCheckingInterval.toMillis * 2))

      // the 0 should be kept for the grace period
      gauge.wrappedCollector.collectValuesFor(label.value, labelValue1.value) shouldBe List(value1)
      gauge.wrappedCollector.collectValuesFor(label.value, labelValue2.value) shouldBe List(0d)

      // the label with 0 should be removed if it's longer than the grace period
      sleep(zeroRemovalTimeout.toMillis + (zerosCheckingInterval.toMillis * 2))

      gauge.wrappedCollector.collectValuesFor(label.value, labelValue1.value) shouldBe List(value1)
      gauge.wrappedCollector.collectValuesFor(label.value, labelValue2.value) shouldBe List.empty
    }

    "not remove the label if 0 value was updated to non-0" in new TestCase {

      gauge.startZeroedValuesCleaning(zeroRemovalTimeout).unsafeRunSync()

      // setting non-0 value
      val labelValue = projectSlugs.generateOne
      val value1     = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue -> value1).unsafeRunSync() shouldBe ()

      gauge.wrappedCollector.collectValuesFor(label.value, labelValue.value) shouldBe List(value1)

      // the value gets 0
      gauge.set(labelValue -> 0d).unsafeRunSync() shouldBe ()

      sleep(zeroRemovalTimeout.toMillis - (zerosCheckingInterval.toMillis * 2))

      // the 0 should be kept for the grace period
      gauge.wrappedCollector.collectValuesFor(label.value, labelValue.value) shouldBe List(0d)

      // before the grace period the value is changed again to non-0
      val value2 = nonNegativeDoubles().generateOne.value
      gauge.set(labelValue -> value2).unsafeRunSync() shouldBe ()

      sleep(zeroRemovalTimeout.toMillis + (zerosCheckingInterval.toMillis * 2))

      gauge.wrappedCollector.collectValuesFor(label.value, labelValue.value) shouldBe List(value2)
    }

    "remove the label after the value was updated to 0" in new TestCase {

      gauge.startZeroedValuesCleaning(zeroRemovalTimeout).unsafeRunSync()

      // setting non-0 value
      val labelValue = projectSlugs.generateOne
      val value      = nonNegativeDoubles().generateOne.value
      gauge.update(labelValue -> value).unsafeRunSync() shouldBe ()

      gauge.wrappedCollector.collectValuesFor(label.value, labelValue.value) shouldBe List(value)

      // the value gets 0
      gauge.update(labelValue -> -value * 2).unsafeRunSync() shouldBe ()

      sleep(zeroRemovalTimeout.toMillis - (zerosCheckingInterval.toMillis * 2))

      // the 0 should be kept for the grace period
      gauge.wrappedCollector.collectValuesFor(label.value, labelValue.value) shouldBe List(0d)

      // after the grace period the label should be removed
      sleep(zeroRemovalTimeout.toMillis + (zerosCheckingInterval.toMillis * 2))

      gauge.wrappedCollector.collectValuesFor(label.value, labelValue.value) shouldBe List.empty
    }

    "remove the label after the value was decremented to 0" in new TestCase {

      gauge.startZeroedValuesCleaning(zeroRemovalTimeout).unsafeRunSync()

      // setting non-0 value
      val labelValue = projectSlugs.generateOne
      val value      = 1d
      gauge.set(labelValue -> value).unsafeRunSync() shouldBe ()

      gauge.wrappedCollector.collectValuesFor(label.value, labelValue.value) shouldBe List(value)

      // the value gets 0
      gauge.decrement(labelValue).unsafeRunSync() shouldBe ()

      sleep(zeroRemovalTimeout.toMillis - (zerosCheckingInterval.toMillis * 2))

      // the 0 should be kept for the grace period
      gauge.wrappedCollector.collectValuesFor(label.value, labelValue.value) shouldBe List(0d)

      // after the grace period the label should be removed
      sleep(zeroRemovalTimeout.toMillis + (zerosCheckingInterval.toMillis * 2))

      gauge.wrappedCollector.collectValuesFor(label.value, labelValue.value) shouldBe List.empty
    }
  }

  private trait TestCase {
    val label = nonBlankStrings().generateOne
    val name  = nonBlankStrings().generateOne
    val help  = nonBlankStrings().generateOne

    val zeroRemovalTimeout    = 500 millis
    val zerosCheckingInterval = 100 millis

    val resetDataFetch = mockFunction[IO[Map[Slug, Double]]]
    val gauge      = new PositiveValuesLabeledGauge[IO, Slug](name, help, label, resetDataFetch, zerosCheckingInterval)
    val underlying = gauge.wrappedCollector
  }

  private lazy val waitingEventsGen: Gen[Map[Slug, Double]] = nonEmptySet {
    for {
      slug  <- projectSlugs
      count <- nonNegativeLongs()
    } yield slug -> count.value.toDouble
  }.map(_.toMap)
}
