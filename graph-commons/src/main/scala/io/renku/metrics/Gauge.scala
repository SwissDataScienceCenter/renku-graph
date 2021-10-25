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

import cats.MonadThrow
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.prometheus.client.{Gauge => LibGauge}

import scala.jdk.CollectionConverters._

trait Gauge[F[_]] {
  protected def gauge: LibGauge

  lazy val name: String = gauge.describe().asScala.head.name
  lazy val help: String = gauge.describe().asScala.head.help
}

trait SingleValueGauge[F[_]] extends Gauge[F] {
  def set(value: Double): F[Unit]
}

trait LabeledGauge[F[_], LabelValue] extends Gauge[F] {
  def set(labelValue:       (LabelValue, Double)): F[Unit]
  def update(labelValue:    (LabelValue, Double)): F[Unit]
  def increment(labelValue: LabelValue):           F[Unit]
  def decrement(labelValue: LabelValue):           F[Unit]
  def reset(): F[Unit]
}

class SingleValueGaugeImpl[F[_]: MonadThrow] private[metrics] (protected val gauge: LibGauge)
    extends SingleValueGauge[F] {
  override def set(value: Double): F[Unit] = MonadThrow[F].catchNonFatal(gauge.set(value))
}

class LabeledGaugeImpl[F[_]: MonadThrow, LabelValue] private[metrics] (
    protected val gauge: LibGauge,
    resetDataFetch:      () => F[Map[LabelValue, Double]]
) extends LabeledGauge[F, LabelValue] {

  override def set(labelValueAndValue: (LabelValue, Double)): F[Unit] =
    MonadThrow[F].catchNonFatal {
      val (labelValue, value) = labelValueAndValue
      gauge.labels(labelValue.toString).set(value)
    }

  override def update(labelValueAndValue: (LabelValue, Double)): F[Unit] =
    MonadThrow[F].catchNonFatal {
      val (labelValue, value) = labelValueAndValue
      val child               = gauge.labels(labelValue.toString)
      child.set(child.get() + value)
    }

  override def increment(labelValue: LabelValue): F[Unit] =
    MonadThrow[F].catchNonFatal {
      gauge.labels(labelValue.toString).inc()
    }

  override def decrement(labelValue: LabelValue): F[Unit] =
    MonadThrow[F].catchNonFatal {
      if (gauge.labels(labelValue.toString).get() != 0)
        gauge.labels(labelValue.toString).dec()
      else ()
    }

  override def reset(): F[Unit] = for {
    newValues <- resetDataFetch()
    _         <- MonadThrow[F].catchNonFatal(gauge.clear())
    _         <- MonadThrow[F].catchNonFatal(newValues foreach set)
  } yield ()
}

object Gauge {

  def apply[F[_]: MonadThrow](
      name:          String Refined NonEmpty,
      help:          String Refined NonEmpty
  )(metricsRegistry: MetricsRegistry): F[SingleValueGauge[F]] = {

    val gaugeBuilder = LibGauge
      .build()
      .name(name.value)
      .help(help.value)

    for {
      gauge <- metricsRegistry register [F, LibGauge, LibGauge.Builder] gaugeBuilder
    } yield new SingleValueGaugeImpl[F](gauge)
  }

  def apply[F[_]: MonadThrow, LabelValue](
      name:          String Refined NonEmpty,
      help:          String Refined NonEmpty,
      labelName:     String Refined NonEmpty
  )(metricsRegistry: MetricsRegistry): F[LabeledGauge[F, LabelValue]] =
    this(name, help, labelName, () => Map.empty[LabelValue, Double].pure[F])(metricsRegistry)

  def apply[F[_]: MonadThrow, LabelValue](
      name:           String Refined NonEmpty,
      help:           String Refined NonEmpty,
      labelName:      String Refined NonEmpty,
      resetDataFetch: () => F[Map[LabelValue, Double]]
  )(metricsRegistry:  MetricsRegistry): F[LabeledGauge[F, LabelValue]] = {

    val gaugeBuilder = LibGauge
      .build()
      .name(name.value)
      .help(help.value)
      .labelNames(labelName.value)

    for {
      gauge <- metricsRegistry register [F, LibGauge, LibGauge.Builder] gaugeBuilder
    } yield new LabeledGaugeImpl[F, LabelValue](gauge, resetDataFetch)
  }
}
