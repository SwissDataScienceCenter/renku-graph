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

package io.renku.eventlog.subscriptions.awaitinggeneration

import cats.Parallel
import cats.effect._
import cats.syntax.all._
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.eventlog.subscriptions._
import io.renku.eventlog.subscriptions.awaitinggeneration.AwaitingGenerationEventEncoder.{encodeEvent, encodePayload}
import io.renku.eventlog.subscriptions.eventdelivery._
import io.renku.eventlog.{EventLogDB, subscriptions}
import io.renku.graph.model.events.CategoryName
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram}
import org.typelevel.log4cats.Logger

private[subscriptions] object SubscriptionCategory {

  val name: CategoryName = CategoryName("AWAITING_GENERATION")

  def apply[F[_]: Async: Parallel: Logger](
      sessionResource:                SessionResource[F, EventLogDB],
      awaitingTriplesGenerationGauge: LabeledGauge[F, projects.Path],
      underTriplesGenerationGauge:    LabeledGauge[F, projects.Path],
      queriesExecTimes:               LabeledHistogram[F, SqlStatement.Name],
      subscriberTracker:              SubscriberTracker[F]
  ): F[subscriptions.SubscriptionCategory[F]] = for {
    subscribers <- Subscribers(name, subscriberTracker)
    eventFetcher <- AwaitingGenerationEventFinder(sessionResource,
                                                  subscribers,
                                                  awaitingTriplesGenerationGauge,
                                                  underTriplesGenerationGauge,
                                                  queriesExecTimes
                    )
    dispatchRecovery <- DispatchRecovery[F]
    eventDelivery <- eventdelivery.EventDelivery[F, AwaitingGenerationEvent](
                       sessionResource,
                       eventDeliveryIdExtractor = (event: AwaitingGenerationEvent) => CompoundEventDeliveryId(event.id),
                       queriesExecTimes
                     )
    eventsDistributor <- EventsDistributor(name,
                                           subscribers,
                                           eventFetcher,
                                           eventDelivery,
                                           EventEncoder(encodeEvent, encodePayload),
                                           dispatchRecovery
                         )
    deserializer <-
      SubscriptionRequestDeserializer[F, SubscriptionCategoryPayload](name, SubscriptionCategoryPayload.apply)
  } yield new SubscriptionCategoryImpl[F, SubscriptionCategoryPayload](name,
                                                                       subscribers,
                                                                       eventsDistributor,
                                                                       deserializer
  )
}
