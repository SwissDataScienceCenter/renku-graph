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

import cats.MonadError
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.prometheus.client.{Gauge => LibGauge}

import scala.jdk.CollectionConverters._

trait Gauge[Interpretation[_]] {
  protected def gauge: LibGauge

  lazy val name: String = gauge.describe().asScala.head.name
  lazy val help: String = gauge.describe().asScala.head.help
}

trait SingleValueGauge[Interpretation[_]] extends Gauge[Interpretation] {

  def set(value: Double): Interpretation[Unit]
}

trait LabeledGauge[Interpretation[_], LabelValue] extends Gauge[Interpretation] {
  def set(labelValue:       (LabelValue, Double)): Interpretation[Unit]
  def increment(labelValue: LabelValue):           Interpretation[Unit]

  def decrement(labelValue: LabelValue): Interpretation[Unit]

  def reset(): Interpretation[Unit]
}

class SingleValueGaugeImpl[Interpretation[_]] private[metrics] (
    protected val gauge: LibGauge
)(implicit ME:           MonadError[Interpretation, Throwable])
    extends SingleValueGauge[Interpretation] {

  override def set(value: Double): Interpretation[Unit] = ME.catchNonFatal {
    gauge.set(value)
  }
}

class LabeledGaugeImpl[Interpretation[_], LabelValue] private[metrics] (
    protected val gauge: LibGauge,
    resetDataFetch:      () => Interpretation[Map[LabelValue, Double]]
)(implicit ME:           MonadError[Interpretation, Throwable])
    extends LabeledGauge[Interpretation, LabelValue] {

  override def set(labelValueAndValue: (LabelValue, Double)): Interpretation[Unit] = ME.catchNonFatal {
    val (labelValue, value) = labelValueAndValue
    gauge.labels(labelValue.toString).set(value)
  }

  def increment(labelValue: LabelValue): Interpretation[Unit] = ME.catchNonFatal {
    gauge.labels(labelValue.toString).inc()
  }

  def decrement(labelValue: LabelValue): Interpretation[Unit] = ME.catchNonFatal {
    if (gauge.labels(labelValue.toString).get() != 0)
      gauge.labels(labelValue.toString).dec()
    else ()
  }

  def reset(): Interpretation[Unit] =
    for {
      newValues <- resetDataFetch()
      _         <- ME.catchNonFatal(gauge.clear())
      _         <- ME.catchNonFatal(newValues foreach set)
    } yield ()
}

object Gauge {

  def apply[Interpretation[_]](
      name: String Refined NonEmpty,
      help: String Refined NonEmpty
  )(
      metricsRegistry: MetricsRegistry[Interpretation]
  )(implicit ME:       MonadError[Interpretation, Throwable]): Interpretation[SingleValueGauge[Interpretation]] = {

    val gaugeBuilder = LibGauge
      .build()
      .name(name.value)
      .help(help.value)

    for {
      gauge <- metricsRegistry register [LibGauge, LibGauge.Builder] gaugeBuilder
    } yield new SingleValueGaugeImpl[Interpretation](gauge)
  }

  def apply[Interpretation[_], LabelValue](
      name:      String Refined NonEmpty,
      help:      String Refined NonEmpty,
      labelName: String Refined NonEmpty
  )(
      metricsRegistry: MetricsRegistry[Interpretation]
  )(implicit ME:       MonadError[Interpretation, Throwable]): Interpretation[LabeledGauge[Interpretation, LabelValue]] =
    this(name, help, labelName, () => Map.empty[LabelValue, Double].pure[Interpretation])(metricsRegistry)

  def apply[Interpretation[_], LabelValue](
      name:           String Refined NonEmpty,
      help:           String Refined NonEmpty,
      labelName:      String Refined NonEmpty,
      resetDataFetch: () => Interpretation[Map[LabelValue, Double]]
  )(
      metricsRegistry: MetricsRegistry[Interpretation]
  )(implicit ME:       MonadError[Interpretation, Throwable]): Interpretation[LabeledGauge[Interpretation, LabelValue]] = {

    val gaugeBuilder = LibGauge
      .build()
      .name(name.value)
      .help(help.value)
      .labelNames(labelName.value)

    for {
      gauge <- metricsRegistry register [LibGauge, LibGauge.Builder] gaugeBuilder
    } yield new LabeledGaugeImpl[Interpretation, LabelValue](gauge, resetDataFetch)
  }
}
