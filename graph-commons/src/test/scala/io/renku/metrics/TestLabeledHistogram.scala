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

import cats.data.NonEmptyList
import cats.effect.IO
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.prometheus.client.{Histogram => LibHistogram}
import org.scalatest.Assertions.fail

import scala.concurrent.duration.FiniteDuration

class TestLabeledHistogram(labelName: String) extends LabeledHistogram[IO] with PrometheusCollector {

  import MetricsTools._

  type Collector = LibHistogram

  override lazy val name: String Refined NonEmpty = "test_metrics"
  override lazy val help: String Refined NonEmpty = "test metrics help"

  private[metrics] val wrappedCollector: LibHistogram = LibHistogram
    .build()
    .name(name.value)
    .help(help.value)
    .labelNames(labelName)
    .buckets(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1)
    .create()

  def verifyExecutionTimeMeasured(forLabelValue: String, other: String*): Unit =
    (forLabelValue +: other) diff this.collectAllSamples.map(_._2) match {
      case Nil     => ()
      case missing => fail(s"Execution time was not measured for '${missing.mkString(", ")}'")
    }

  def verifyExecutionTimeMeasured(forLabelValues: NonEmptyList[String]): Unit =
    verifyExecutionTimeMeasured(forLabelValues.head, forLabelValues.tail: _*)

  def verifyNoInteractions(): Unit = {
    if (this.collectAllSamples.nonEmpty)
      fail(
        "Expected no interaction with histogram but " +
          s"execution time was measured for ${this.collectAllSamples.map(_._2).toSet.mkString(",")}"
      )
    ()
  }

  override def observe(labelValue: String, amt: FiniteDuration): IO[Unit] =
    observe(labelValue, amt.toSeconds.toDouble)

  override def observe(labelValue: String, amt: Double): IO[Unit] =
    IO {
      wrappedCollector.labels(labelValue).observe(amt)
    }
}

object TestLabeledHistogram {

  def apply[LabelValue](labelName: String): TestLabeledHistogram =
    new TestLabeledHistogram(labelName)
}
