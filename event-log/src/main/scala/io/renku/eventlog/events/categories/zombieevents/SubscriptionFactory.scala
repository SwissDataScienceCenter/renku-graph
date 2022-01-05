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

package io.renku.eventlog.events.categories.zombieevents

import cats.effect.Concurrent
import cats.effect.kernel.{Async, Spawn, Temporal}
import cats.syntax.all._
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.eventlog.{EventLogDB, Microservice}
import io.renku.events.consumers.EventHandler
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.subscriptions.SubscriptionPayloadComposer.categoryAndUrlPayloadsComposerFactory
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram}
import org.typelevel.log4cats.Logger

object SubscriptionFactory {

  def apply[F[_]: Async: Spawn: Concurrent: Temporal: Logger](
      sessionResource:                    SessionResource[F, EventLogDB],
      awaitingTriplesGenerationGauge:     LabeledGauge[F, projects.Path],
      underTriplesGenerationGauge:        LabeledGauge[F, projects.Path],
      awaitingTriplesTransformationGauge: LabeledGauge[F, projects.Path],
      underTriplesTransformationGauge:    LabeledGauge[F, projects.Path],
      queriesExecTimes:                   LabeledHistogram[F, SqlStatement.Name]
  ): F[(EventHandler[F], SubscriptionMechanism[F])] = for {
    subscriptionMechanism <- SubscriptionMechanism(
                               categoryName,
                               categoryAndUrlPayloadsComposerFactory(Microservice.ServicePort, Microservice.Identifier)
                             )
    handler <- EventHandler(
                 sessionResource,
                 queriesExecTimes,
                 awaitingTriplesGenerationGauge,
                 underTriplesGenerationGauge,
                 awaitingTriplesTransformationGauge,
                 underTriplesTransformationGauge
               )
  } yield handler -> subscriptionMechanism
}
