/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import cats.MonadError
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.prometheus.client.{Histogram => LibHistogram}

import scala.jdk.CollectionConverters._

trait Histogram[Interpretation[_]] {
  protected def histogram: LibHistogram

  lazy val name: String = histogram.describe().asScala.head.name
  lazy val help: String = histogram.describe().asScala.head.help
}

object Histogram {

  trait Timer[Interpretation[_]] {
    def observeDuration: Interpretation[Double]
  }
  final class TimerImpl[Interpretation[_]] private[metrics] (
      timer:     LibHistogram.Timer
  )(implicit ME: MonadError[Interpretation, Throwable])
      extends Timer[Interpretation] {
    def observeDuration: Interpretation[Double] = ME.catchNonFatal {
      timer.observeDuration()
    }
  }

  def apply[Interpretation[_], LabelValue](
      name:      String Refined NonEmpty,
      help:      String Refined NonEmpty,
      labelName: String Refined NonEmpty,
      buckets:   Seq[Double]
  )(
      metricsRegistry: MetricsRegistry[Interpretation]
  )(implicit
      ME: MonadError[Interpretation, Throwable]
  ): Interpretation[LabeledHistogram[Interpretation, LabelValue]] = {

    val builder = LibHistogram
      .build()
      .name(name.value)
      .help(help.value)
      .labelNames(labelName.value)
      .buckets(buckets: _*)

    for {
      histogram <- metricsRegistry register [LibHistogram, LibHistogram.Builder] builder
    } yield new LabeledHistogramImpl[Interpretation, LabelValue](histogram)
  }
}

trait LabeledHistogram[Interpretation[_], LabelValue] extends Histogram[Interpretation] {
  def startTimer(labelValue: LabelValue): Interpretation[Histogram.Timer[Interpretation]]
}

class LabeledHistogramImpl[Interpretation[_], LabelValue] private[metrics] (
    protected val histogram: LibHistogram
)(implicit ME:               MonadError[Interpretation, Throwable])
    extends LabeledHistogram[Interpretation, LabelValue] {

  override def startTimer(labelValue: LabelValue): Interpretation[Histogram.Timer[Interpretation]] = ME.catchNonFatal {
    new Histogram.TimerImpl(histogram.labels(labelValue.toString).startTimer())
  }
}
