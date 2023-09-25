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

import cats.syntax.all._
import cats.{Applicative, MonadThrow}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.prometheus.client.{Histogram => LibHistogram}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

trait Histogram[F[_]] extends MetricsCollector {
  def observe(maybeLabel: Option[String], duration: FiniteDuration)(implicit L: Logger[F], A: Applicative[F]): F[Unit] =
    this -> maybeLabel match {
      case (h: SingleValueHistogram[F]) -> None    => h.observe(duration)
      case (h: LabeledHistogram[F]) -> Some(label) => h.observe(label, duration)
      case (h: SingleValueHistogram[F]) -> Some(label) =>
        L.error(s"Label $label sent for a Single Value Histogram ${h.name}")
      case _ => ().pure[F]
    }
}

trait SingleValueHistogram[F[_]] extends Histogram[F] {
  def observe(amt: FiniteDuration): F[Unit]
  def observe(amt: Double):         F[Unit]
}

class SingleValueHistogramImpl[F[_]: MonadThrow](val name: String Refined NonEmpty,
                                                 val help:     String Refined NonEmpty,
                                                 maybeBuckets: Option[Seq[Double]]
) extends SingleValueHistogram[F]
    with PrometheusCollector {

  type Collector = LibHistogram

  private[metrics] override lazy val wrappedCollector: LibHistogram = {
    val builder = LibHistogram
      .build()
      .name(name.value)
      .help(help.value)
    maybeBuckets
      .fold(ifEmpty = builder)(buckets => builder.buckets(buckets: _*))
      .create()
  }

  override def observe(amt: FiniteDuration): F[Unit] =
    observe(amt.toMillis.toDouble / 1000)

  override def observe(amt: Double): F[Unit] = MonadThrow[F].catchNonFatal {
    wrappedCollector.observe(amt)
  }
}

trait LabeledHistogram[F[_]] extends Histogram[F] {
  def observe(labelValue: String, amt: FiniteDuration): F[Unit]
  def observe(labelValue: String, amt: Double):         F[Unit]
}

class LabeledHistogramImpl[F[_]: MonadThrow](val name: String Refined NonEmpty,
                                             val help:       String Refined NonEmpty,
                                             labelName:      String Refined NonEmpty,
                                             maybeBuckets:   Option[Seq[Double]],
                                             maybeThreshold: Option[Duration] = None
) extends LabeledHistogram[F]
    with PrometheusCollector {

  type Collector = LibHistogram

  private[metrics] override lazy val wrappedCollector: LibHistogram = {
    val builder = LibHistogram
      .build()
      .name(name.value)
      .help(help.value)
      .labelNames(labelName.value)
    maybeBuckets
      .fold(ifEmpty = builder)(buckets => builder.buckets(buckets: _*))
      .create()
  }

  private val maybeThresholdMillis = maybeThreshold.map(_.toMillis.toDouble)

  override def observe(labelValue: String, amt: FiniteDuration): F[Unit] =
    observe(labelValue, amt.toMillis.toDouble / 1000)

  override def observe(labelValue: String, amt: Double): F[Unit] =
    MonadThrow[F].catchNonFatal {
      if (maybeThresholdMillis.fold(ifEmpty = true)((amt * 1000d) >= _))
        wrappedCollector.labels(labelValue).observe(amt)
      else ()
    }
}

object Histogram {

  def apply[F[_]: MonadThrow: MetricsRegistry](
      name:    String Refined NonEmpty,
      help:    String Refined NonEmpty,
      buckets: Seq[Double]
  ): F[SingleValueHistogram[F]] =
    MetricsRegistry[F]
      .register(new SingleValueHistogramImpl[F](name, help, buckets.some))
      .widen

  def apply[F[_]: MonadThrow: MetricsRegistry](
      name: String Refined NonEmpty,
      help: String Refined NonEmpty
  ): F[SingleValueHistogram[F]] =
    MetricsRegistry[F]
      .register(new SingleValueHistogramImpl[F](name, help, maybeBuckets = None))
      .widen

  def apply[F[_]: MonadThrow: MetricsRegistry](
      name:           String Refined NonEmpty,
      help:           String Refined NonEmpty,
      labelName:      String Refined NonEmpty,
      buckets:        Seq[Double],
      maybeThreshold: Option[Duration]
  ): F[LabeledHistogram[F]] =
    MetricsRegistry[F]
      .register(new LabeledHistogramImpl[F](name, help, labelName, buckets.some, maybeThreshold))
      .widen

  def apply[F[_]: MonadThrow: MetricsRegistry](
      name:           String Refined NonEmpty,
      help:           String Refined NonEmpty,
      labelName:      String Refined NonEmpty,
      maybeThreshold: Option[Duration]
  ): F[LabeledHistogram[F]] =
    MetricsRegistry[F]
      .register(new LabeledHistogramImpl[F](name, help, labelName, maybeBuckets = None, maybeThreshold))
      .widen
}
