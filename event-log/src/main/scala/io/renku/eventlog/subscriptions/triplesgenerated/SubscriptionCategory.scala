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

package io.renku.eventlog.subscriptions.triplesgenerated

import cats.effect.Async
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.subscriptions
import io.renku.eventlog.subscriptions._
import io.renku.eventlog.subscriptions.eventdelivery._
import io.renku.eventlog.subscriptions.triplesgenerated.TriplesGeneratedEventEncoder.{encodeEvent, encodePayload}
import io.renku.events.CategoryName
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram, MetricsRegistry}
import org.typelevel.log4cats.Logger

private[subscriptions] object SubscriptionCategory {
  val categoryName: CategoryName = CategoryName("TRIPLES_GENERATED")

  def apply[F[_]: Async: SessionResource: UrlAndIdSubscriberTracker: Logger: MetricsRegistry](
      awaitingTransformationGauge: LabeledGauge[F, projects.Path],
      underTransformationGauge:    LabeledGauge[F, projects.Path],
      queriesExecTimes:            LabeledHistogram[F]
  ): F[subscriptions.SubscriptionCategory[F]] = for {
    subscribers      <- UrlAndIdSubscribers[F](categoryName)
    eventFetcher     <- EventFinder(awaitingTransformationGauge, underTransformationGauge, queriesExecTimes)
    dispatchRecovery <- DispatchRecovery[F]
    eventDelivery <- EventDelivery[F, TriplesGeneratedEvent](
                       eventDeliveryIdExtractor = (event: TriplesGeneratedEvent) => CompoundEventDeliveryId(event.id),
                       queriesExecTimes
                     )
    eventsDistributor <- EventsDistributor(categoryName,
                                           subscribers,
                                           eventFetcher,
                                           eventDelivery,
                                           EventEncoder(encodeEvent, encodePayload),
                                           dispatchRecovery
                         )
    deserializer <-
      SubscriptionRequestDeserializer[F, SubscriptionCategoryPayload](categoryName, SubscriptionCategoryPayload.apply)
  } yield new SubscriptionCategoryImpl[F, SubscriptionCategoryPayload](categoryName,
                                                                       subscribers,
                                                                       eventsDistributor,
                                                                       deserializer
  )
}
