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

package io.renku.eventlog.events.categories.statuschange

import cats.effect.{ContextShift, IO, Timer}
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.eventlog.EventLogDB
import io.renku.events.consumers.EventHandler
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

object SubscriptionFactory {
  def apply(sessionResource:                    SessionResource[IO, EventLogDB],
            awaitingTriplesGenerationGauge:     LabeledGauge[IO, projects.Path],
            underTriplesGenerationGauge:        LabeledGauge[IO, projects.Path],
            awaitingTriplesTransformationGauge: LabeledGauge[IO, projects.Path],
            underTriplesTransformationGauge:    LabeledGauge[IO, projects.Path],
            queriesExecTimes:                   LabeledHistogram[IO, SqlStatement.Name]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO],
      logger:           Logger[IO]
  ): IO[(EventHandler[IO], SubscriptionMechanism[IO])] = for {
    handler <- EventHandler(
                 sessionResource,
                 queriesExecTimes,
                 awaitingTriplesGenerationGauge,
                 underTriplesGenerationGauge,
                 awaitingTriplesTransformationGauge,
                 underTriplesTransformationGauge
               )
  } yield handler -> SubscriptionMechanism.noOpSubscriptionMechanism(categoryName)

}
