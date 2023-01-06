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

import cats.MonadThrow
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.prometheus.client.{Histogram => LibHistogram}
import io.renku.metrics

import scala.concurrent.duration.Duration

sealed trait Histogram[F[_]] extends MetricsCollector

trait SingleValueHistogram[F[_]] extends Histogram[F] {
  def startTimer(): F[Histogram.Timer[F]]
}

object SingleValueHistogram {

  final class NoThresholdTimerImpl[F[_]: MonadThrow] private[metrics] (timer: LibHistogram.Timer)
      extends Histogram.Timer[F] {

    def observeDuration: F[Double] = MonadThrow[F].catchNonFatal {
      timer.observeDuration()
    }
  }
}

class SingleValueHistogramImpl[F[_]: MonadThrow](val name: String Refined NonEmpty,
                                                 val help: String Refined NonEmpty,
                                                 buckets:  Seq[Double]
) extends SingleValueHistogram[F]
    with PrometheusCollector {

  type Collector = LibHistogram

  private[metrics] override lazy val wrappedCollector: LibHistogram =
    LibHistogram
      .build()
      .name(name.value)
      .help(help.value)
      .buckets(buckets: _*)
      .create()

  override def startTimer(): F[Histogram.Timer[F]] = MonadThrow[F].catchNonFatal {
    new metrics.SingleValueHistogram.NoThresholdTimerImpl(wrappedCollector.startTimer())
  }
}

trait LabeledHistogram[F[_]] extends Histogram[F] {
  def startTimer(labelValue: String): F[Histogram.Timer[F]]
}

object LabeledHistogram {

  final class NoThresholdTimerImpl[F[_]: MonadThrow] private[metrics] (timer: LibHistogram.Timer)
      extends Histogram.Timer[F] {

    def observeDuration: F[Double] = MonadThrow[F].catchNonFatal {
      timer.observeDuration()
    }
  }

  final class ThresholdTimerImpl[F[_]: MonadThrow] private[metrics] (timer: LibHistogram.Timer,
                                                                     labelValue:       String,
                                                                     wrappedCollector: LibHistogram,
                                                                     thresholdMillis:  Double
  ) extends Histogram.Timer[F] {

    def observeDuration: F[Double] = MonadThrow[F].catchNonFatal {
      timer.observeDuration() match {
        case d if (d * 1000d) >= thresholdMillis => d
        case d =>
          wrappedCollector.remove(labelValue)
          d
      }
    }
  }
}

class LabeledHistogramImpl[F[_]: MonadThrow](val name: String Refined NonEmpty,
                                             val help:       String Refined NonEmpty,
                                             labelName:      String Refined NonEmpty,
                                             buckets:        Seq[Double],
                                             maybeThreshold: Option[Duration] = None
) extends LabeledHistogram[F]
    with PrometheusCollector {

  type Collector = LibHistogram

  private[metrics] override lazy val wrappedCollector: LibHistogram =
    LibHistogram
      .build()
      .name(name.value)
      .help(help.value)
      .labelNames(labelName.value)
      .buckets(buckets: _*)
      .create()

  private val maybeThresholdMillis = maybeThreshold.map(_.toMillis.toDouble)

  override def startTimer(labelValue: String): F[Histogram.Timer[F]] = MonadThrow[F].catchNonFatal {
    maybeThresholdMillis match {
      case None => new metrics.LabeledHistogram.NoThresholdTimerImpl(wrappedCollector.labels(labelValue).startTimer())
      case Some(threshold) =>
        new metrics.LabeledHistogram.ThresholdTimerImpl(wrappedCollector.labels(labelValue).startTimer(),
                                                        labelValue,
                                                        wrappedCollector,
                                                        threshold
        )
    }
  }
}

object Histogram {

  trait Timer[F[_]] {
    def observeDuration: F[Double]
  }

  def apply[F[_]: MonadThrow: MetricsRegistry](
      name:    String Refined NonEmpty,
      help:    String Refined NonEmpty,
      buckets: Seq[Double]
  ): F[SingleValueHistogram[F]] =
    MetricsRegistry[F]
      .register(new SingleValueHistogramImpl[F](name, help, buckets))
      .widen

  def apply[F[_]: MonadThrow: MetricsRegistry](
      name:      String Refined NonEmpty,
      help:      String Refined NonEmpty,
      labelName: String Refined NonEmpty,
      buckets:   Seq[Double]
  ): F[LabeledHistogram[F]] =
    MetricsRegistry[F]
      .register(new LabeledHistogramImpl[F](name, help, labelName, buckets))
      .widen
}
