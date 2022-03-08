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

sealed trait Histogram[F[_]] extends MetricsCollector

trait SingleValueHistogram[F[_]] extends Histogram[F] {
  def startTimer(): F[Histogram.Timer[F]]
}

class SingleValueHistogramImpl[F[_]: MonadThrow] private[metrics] (val wrappedCollector: LibHistogram)
    extends SingleValueHistogram[F]
    with PrometheusCollector {

  type Collector = LibHistogram
  override lazy val name: String = wrappedCollector.describe().asScala.head.name
  override lazy val help: String = wrappedCollector.describe().asScala.head.help

  override def startTimer(): F[Histogram.Timer[F]] = MonadThrow[F].catchNonFatal {
    new Histogram.TimerImpl(wrappedCollector.startTimer())
  }
}

trait LabeledHistogram[F[_]] extends Histogram[F] {
  def startTimer(labelValue: String): F[Histogram.Timer[F]]
}

class LabeledHistogramImpl[F[_]: MonadThrow] private[metrics] (val wrappedCollector: LibHistogram)
    extends LabeledHistogram[F]
    with PrometheusCollector {

  type Collector = LibHistogram
  override lazy val name: String = wrappedCollector.describe().asScala.head.name
  override lazy val help: String = wrappedCollector.describe().asScala.head.help

  override def startTimer(labelValue: String): F[Histogram.Timer[F]] = MonadThrow[F].catchNonFatal {
    new Histogram.TimerImpl(wrappedCollector.labels(labelValue).startTimer())
  }
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

  def apply[F[_]: MonadThrow: MetricsRegistry](
      name:    String Refined NonEmpty,
      help:    String Refined NonEmpty,
      buckets: Seq[Double]
  ): F[SingleValueHistogram[F]] = {

    val histogram = new SingleValueHistogramImpl[F](
      LibHistogram
        .build()
        .name(name.value)
        .help(help.value)
        .buckets(buckets: _*)
        .create()
    )

    MetricsRegistry[F].register(histogram).map(_ => histogram)
  }

  def apply[F[_]: MonadThrow: MetricsRegistry](
      name:      String Refined NonEmpty,
      help:      String Refined NonEmpty,
      labelName: String Refined NonEmpty,
      buckets:   Seq[Double]
  ): F[LabeledHistogram[F]] = {

    val histogram = new LabeledHistogramImpl[F](
      LibHistogram
        .build()
        .name(name.value)
        .help(help.value)
        .labelNames(labelName.value)
        .buckets(buckets: _*)
        .create()
    )

    MetricsRegistry[F].register(histogram).map(_ => histogram)
  }
}
