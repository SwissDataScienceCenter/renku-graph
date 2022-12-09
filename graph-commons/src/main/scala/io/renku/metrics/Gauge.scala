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
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty

trait Gauge[F[_]] extends MetricsCollector

object Gauge {

  def apply[F[_]: MonadThrow: MetricsRegistry](
      name: String Refined NonEmpty,
      help: String Refined NonEmpty
  ): F[SingleValueGauge[F]] =
    MetricsRegistry[F].register(new SingleValueGaugeImpl[F](name, help)).widen

  def apply[F[_]: Async: MetricsRegistry, LabelValue](
      name:      String Refined NonEmpty,
      help:      String Refined NonEmpty,
      labelName: String Refined NonEmpty
  ): F[LabeledGauge[F, LabelValue]] =
    this(name, help, labelName, () => Map.empty[LabelValue, Double].pure[F])

  def apply[F[_]: Async: MetricsRegistry, LabelValue](
      name:           String Refined NonEmpty,
      help:           String Refined NonEmpty,
      labelName:      String Refined NonEmpty,
      resetDataFetch: () => F[Map[LabelValue, Double]]
  ): F[LabeledGauge[F, LabelValue]] =
    MetricsRegistry[F]
      .register(new PositiveValuesLabeledGauge[F, LabelValue](name, help, labelName, resetDataFetch))
      .widen
}
