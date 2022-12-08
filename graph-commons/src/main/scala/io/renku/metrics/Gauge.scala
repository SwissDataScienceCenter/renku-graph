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
import io.prometheus.client.{Gauge => LibGauge}

trait Gauge[F[_]] extends MetricsCollector

trait SingleValueGauge[F[_]] extends Gauge[F] {
  def set(value: Double): F[Unit]
}

trait LabeledGauge[F[_], LabelValue] extends Gauge[F] {
  def set(labelValue:       (LabelValue, Double)): F[Unit]
  def update(labelValue:    (LabelValue, Double)): F[Unit]
  def increment(labelValue: LabelValue):           F[Unit]
  def decrement(labelValue: LabelValue):           F[Unit]
  def reset(): F[Unit]
  def clear(): F[Unit]
}

class SingleValueGaugeImpl[F[_]: MonadThrow](val name: String Refined NonEmpty, val help: String Refined NonEmpty)
    extends SingleValueGauge[F]
    with PrometheusCollector {

  type Collector = LibGauge

  private[metrics] lazy val wrappedCollector: Collector =
    LibGauge
      .build()
      .name(name.value)
      .help(help.value)
      .create()

  override def set(value: Double): F[Unit] = MonadThrow[F].catchNonFatal(wrappedCollector set value)
}

class LabeledGaugeImpl[F[_]: MonadThrow, LabelValue](val name: String Refined NonEmpty,
                                                     val help:       String Refined NonEmpty,
                                                     labelName:      String Refined NonEmpty,
                                                     resetDataFetch: () => F[Map[LabelValue, Double]]
) extends LabeledGauge[F, LabelValue]
    with PrometheusCollector {

  type Collector = LibGauge

  private[metrics] lazy val wrappedCollector: Collector =
    LibGauge
      .build()
      .name(name.value)
      .help(help.value)
      .labelNames(labelName.value)
      .create()

  override def set(labelValueAndValue: (LabelValue, Double)): F[Unit] = MonadThrow[F].catchNonFatal {
    val (labelValue, value) = labelValueAndValue
    wrappedCollector.labels(labelValue.toString).set(if (value < 0) 0d else value)
  }

  override def update(labelValueAndValue: (LabelValue, Double)): F[Unit] = MonadThrow[F].catchNonFatal {
    val (labelValue, value) = labelValueAndValue
    val child               = wrappedCollector.labels(labelValue.toString)
    child.get() + value match {
      case v if v < 0 => child.set(0d)
      case v          => child.set(v)
    }
  }

  override def increment(labelValue: LabelValue): F[Unit] = MonadThrow[F].catchNonFatal {
    wrappedCollector.labels(labelValue.toString).inc()
  }

  override def decrement(labelValue: LabelValue): F[Unit] = MonadThrow[F].catchNonFatal {
    if (wrappedCollector.labels(labelValue.toString).get() != 0)
      wrappedCollector.labels(labelValue.toString).dec()
    else ()
  }

  override def reset(): F[Unit] = for {
    newValues <- resetDataFetch()
    _         <- clear()
    _         <- MonadThrow[F].catchNonFatal(newValues foreach set)
  } yield ()

  override def clear(): F[Unit] = MonadThrow[F].catchNonFatal(wrappedCollector.clear())
}

object Gauge {

  def apply[F[_]: MonadThrow: MetricsRegistry](
      name: String Refined NonEmpty,
      help: String Refined NonEmpty
  ): F[SingleValueGauge[F]] =
    MetricsRegistry[F].register(new SingleValueGaugeImpl[F](name, help)).widen

  def apply[F[_]: MonadThrow: MetricsRegistry, LabelValue](
      name:      String Refined NonEmpty,
      help:      String Refined NonEmpty,
      labelName: String Refined NonEmpty
  ): F[LabeledGauge[F, LabelValue]] =
    this(name, help, labelName, () => Map.empty[LabelValue, Double].pure[F])

  def apply[F[_]: MonadThrow: MetricsRegistry, LabelValue](
      name:           String Refined NonEmpty,
      help:           String Refined NonEmpty,
      labelName:      String Refined NonEmpty,
      resetDataFetch: () => F[Map[LabelValue, Double]]
  ): F[LabeledGauge[F, LabelValue]] =
    MetricsRegistry[F]
      .register(new LabeledGaugeImpl[F, LabelValue](name, help, labelName, resetDataFetch))
      .widen
}
