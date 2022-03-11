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

import cats.data.NonEmptyList
import cats.effect.IO
import io.prometheus.client.{Histogram => LibHistogram}
import org.scalatest.Assertions.fail

import scala.jdk.CollectionConverters._

class TestLabeledHistogram(val wrappedCollector: LibHistogram) extends LabeledHistogram[IO] {

  import MetricsTools._

  override lazy val name: String = wrappedCollector.describe().asScala.head.name
  override lazy val help: String = wrappedCollector.describe().asScala.head.help

  def verifyExecutionTimeMeasured(forLabelValue: String, other: String*): Unit =
    (forLabelValue +: other) diff wrappedCollector.collectAllSamples.map(_._2) match {
      case Nil     => ()
      case missing => fail(s"Execution time was not measured for '${missing.mkString(", ")}'")
    }

  def verifyExecutionTimeMeasured(forLabelValues: NonEmptyList[String]): Unit =
    verifyExecutionTimeMeasured(forLabelValues.head, forLabelValues.tail: _*)

  def verifyNoInteractions(): Unit = {
    if (wrappedCollector.collectAllSamples.nonEmpty)
      fail(
        "Expected no interaction with histogram but " +
          s"execution time was measured for ${wrappedCollector.collectAllSamples.map(_._2).toSet.mkString(",")}"
      )
    ()
  }

  override def startTimer(labelValue: String): IO[Histogram.Timer[IO]] = IO {
    new Histogram.TimerImpl(wrappedCollector.labels(labelValue).startTimer())
  }
}

object TestLabeledHistogram {

  def apply[LabelValue](labelName: String): TestLabeledHistogram =
    new TestLabeledHistogram(
      LibHistogram
        .build()
        .name("test_metrics")
        .help("test metrics help")
        .labelNames(labelName)
        .buckets(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1)
        .create()
    )
}
