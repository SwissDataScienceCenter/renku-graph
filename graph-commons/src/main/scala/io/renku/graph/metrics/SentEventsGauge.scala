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

package io.renku.graph.metrics

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.events.CategoryName
import io.renku.metrics.{LabeledGauge, MetricsRegistry, PositiveValuesLabeledGauge}

trait SentEventsGauge[F[_]] extends LabeledGauge[F, CategoryName]

object SentEventsGauge {

  def apply[F[_]: Async: MetricsRegistry]: F[SentEventsGauge[F]] = MetricsRegistry[F].register {
    new PositiveValuesLabeledGauge[F, CategoryName](
      name = "sent_events_count",
      help = "Number of sent Events",
      labelName = "category_mame",
      resetDataFetch = () => Map.empty[CategoryName, Double].pure[F]
    ) with SentEventsGauge[F]
  }.widen
}
