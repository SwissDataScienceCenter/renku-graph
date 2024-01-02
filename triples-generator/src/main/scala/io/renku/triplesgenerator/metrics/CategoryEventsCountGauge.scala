/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.metrics

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.events.CategoryName
import io.renku.metrics.{LabeledGauge, MetricsRegistry, PositiveValuesLabeledGauge}

private trait CategoryEventsCountGauge[F[_]] extends LabeledGauge[F, CategoryName]

private object CategoryEventsCountGauge {

  def apply[F[_]: Async: MetricsRegistry]: F[CategoryEventsCountGauge[F]] =
    MetricsRegistry[F]
      .register(
        new PositiveValuesLabeledGauge[F, CategoryName](
          name = "category_name_events_count",
          help = "Number of events waiting for processing per Category Name.",
          labelName = "category_name",
          resetDataFetch = () => Async[F].pure(Map.empty)
        ) with CategoryEventsCountGauge[F]
      )
      .widen
}
