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

import cats.effect.IO
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.prometheus.client.{Histogram => LibHistogram}
import org.scalatest.Assertions.fail

class TestLabeledHistogram[LabelValue](
    protected val histogram: LibHistogram
) extends LabeledHistogram[IO, LabelValue] {

  import MetricsTools._

  def verifyExecutionTimeMeasured(forLabelValue: LabelValue, other: LabelValue*): Unit =
    (forLabelValue +: other).map(_.toString) diff histogram.collectAllSamples.map(_._2) match {
      case Nil     => ()
      case missing => fail(s"Execution time was not measured for '${missing.mkString(", ")}'")
    }

  def verifyNoInteractions(): Unit = {
    if (histogram.collectAllSamples.nonEmpty)
      fail(
        "Expected no interaction with histogram but " +
          s"execution time was measured for ${histogram.collectAllSamples.map(_._2).toSet.mkString(",")}"
      )
    ()
  }

  override def startTimer(labelValue: LabelValue): IO[Histogram.Timer[IO]] = IO {
    new Histogram.TimerImpl(histogram.labels(labelValue.toString).startTimer())
  }
}

object TestLabeledHistogram {

  def apply[LabelValue](labelName: String Refined NonEmpty): TestLabeledHistogram[LabelValue] =
    new TestLabeledHistogram[LabelValue](
      LibHistogram
        .build()
        .name("test_metrics")
        .help("test metrics help")
        .labelNames(labelName.value)
        .buckets(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1)
        .create()
    )
}
