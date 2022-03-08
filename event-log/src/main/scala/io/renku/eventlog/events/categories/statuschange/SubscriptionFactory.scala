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

package io.renku.eventlog.events.categories.statuschange

import cats.effect.kernel.Async
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.eventlog.EventLogDB
import io.renku.events.consumers.EventHandler
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram, MetricsRegistry}
import org.typelevel.log4cats.Logger

object SubscriptionFactory {
  def apply[F[_]: Async: Logger: MetricsRegistry](
      sessionResource:                    SessionResource[F, EventLogDB],
      eventsQueue:                        StatusChangeEventsQueue[F],
      awaitingTriplesGenerationGauge:     LabeledGauge[F, projects.Path],
      underTriplesGenerationGauge:        LabeledGauge[F, projects.Path],
      awaitingTriplesTransformationGauge: LabeledGauge[F, projects.Path],
      underTriplesTransformationGauge:    LabeledGauge[F, projects.Path],
      awaitingDeletionGauge:              LabeledGauge[F, projects.Path],
      deletingGauge:                      LabeledGauge[F, projects.Path],
      queriesExecTimes:                   LabeledHistogram[F]
  ): F[(EventHandler[F], SubscriptionMechanism[F])] = for {
    handler <- EventHandler(
                 sessionResource,
                 eventsQueue,
                 queriesExecTimes,
                 awaitingTriplesGenerationGauge,
                 underTriplesGenerationGauge,
                 awaitingTriplesTransformationGauge,
                 underTriplesTransformationGauge,
                 awaitingDeletionGauge,
                 deletingGauge
               )
  } yield handler -> SubscriptionMechanism.noOpSubscriptionMechanism(categoryName)
}
