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
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.prometheus.client.{Histogram => LibHistogram}

import scala.jdk.CollectionConverters._

trait Histogram[F[_]] {
  protected def histogram: LibHistogram

  lazy val name: String = histogram.describe().asScala.head.name
  lazy val help: String = histogram.describe().asScala.head.help
}

object Histogram {

  trait Timer[F[_]] {
    def observeDuration: F[Double]
  }
  final class TimerImpl[F[_]: MonadThrow] private[metrics] (timer: LibHistogram.Timer) extends Timer[F] {
    def observeDuration: F[Double] = MonadThrow[F].catchNonFatal {
      timer.observeDuration()
    }
  }

  def apply[F[_]: MonadThrow: MetricsRegistry, LabelValue](
      name:      String Refined NonEmpty,
      help:      String Refined NonEmpty,
      labelName: String Refined NonEmpty,
      buckets:   Seq[Double]
  ): F[LabeledHistogram[F, LabelValue]] = {

    val builder = LibHistogram
      .build()
      .name(name.value)
      .help(help.value)
      .labelNames(labelName.value)
      .buckets(buckets: _*)

    for {
      histogram <- MetricsRegistry[F] register [LibHistogram, LibHistogram.Builder] builder
    } yield new LabeledHistogramImpl[F, LabelValue](histogram)
  }
}

trait LabeledHistogram[F[_], LabelValue] extends Histogram[F] {
  def startTimer(labelValue: LabelValue): F[Histogram.Timer[F]]
}

class LabeledHistogramImpl[F[_]: MonadThrow, LabelValue] private[metrics] (
    protected val histogram: LibHistogram
) extends LabeledHistogram[F, LabelValue] {

  override def startTimer(labelValue: LabelValue): F[Histogram.Timer[F]] = MonadThrow[F].catchNonFatal {
    new Histogram.TimerImpl(histogram.labels(labelValue.toString).startTimer())
  }
}
