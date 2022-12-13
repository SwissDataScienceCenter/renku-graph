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
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty

trait SingleValueGauge[F[_]] extends Gauge[F] {
  def set(value: Double): F[Unit]
}

class SingleValueGaugeImpl[F[_]: MonadThrow](val name: String Refined NonEmpty, val help: String Refined NonEmpty)
    extends SingleValueGauge[F]
    with PrometheusCollector {
  import io.prometheus.client.{Gauge => LibGauge}

  type Collector = LibGauge

  private[metrics] lazy val wrappedCollector: Collector =
    LibGauge
      .build()
      .name(name.value)
      .help(help.value)
      .create()

  override def set(value: Double): F[Unit] = MonadThrow[F].catchNonFatal(wrappedCollector set value)
}
