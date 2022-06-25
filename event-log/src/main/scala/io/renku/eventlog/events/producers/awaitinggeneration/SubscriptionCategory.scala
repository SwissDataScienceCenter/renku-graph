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

package io.renku.eventlog.events.producers
package awaitinggeneration

import AwaitingGenerationEventEncoder.{encodeEvent, encodePayload}
import cats.Parallel
import cats.effect._
import cats.syntax.all._
import eventdelivery._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.events.producers
import io.renku.eventlog.events.producers.UrlAndIdSubscriberTracker
import io.renku.events.CategoryName
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram, MetricsRegistry}
import org.typelevel.log4cats.Logger

private[producers] object SubscriptionCategory {

  val categoryName: CategoryName = CategoryName("AWAITING_GENERATION")

  def apply[F[_]: Async: Parallel: SessionResource: UrlAndIdSubscriberTracker: Logger: MetricsRegistry](
      awaitingGenerationGauge: LabeledGauge[F, projects.Path],
      underGenerationGauge:    LabeledGauge[F, projects.Path],
      queriesExecTimes:        LabeledHistogram[F]
  ): F[producers.SubscriptionCategory[F]] = UrlAndIdSubscribers[F](categoryName)
    .flatMap { implicit subscribers =>
      for {
        eventFetcher     <- EventFinder(awaitingGenerationGauge, underGenerationGauge, queriesExecTimes)
        dispatchRecovery <- DispatchRecovery[F]
        eventDelivery <- eventdelivery.EventDelivery[F, AwaitingGenerationEvent](
                           eventDeliveryIdExtractor =
                             (event: AwaitingGenerationEvent) => CompoundEventDeliveryId(event.id),
                           queriesExecTimes
                         )
        eventsDistributor <- EventsDistributor(categoryName,
                                               subscribers,
                                               eventFetcher,
                                               eventDelivery,
                                               EventEncoder(encodeEvent, encodePayload),
                                               dispatchRecovery
                             )
        deserializer <- UrlAndIdSubscriptionDeserializer[F, SubscriptionPayload](
                          categoryName,
                          SubscriptionPayload.apply
                        )
      } yield new SubscriptionCategoryImpl[F, SubscriptionPayload](categoryName,
                                                                   subscribers,
                                                                   eventsDistributor,
                                                                   deserializer
      )
    }
    .widen[producers.SubscriptionCategory[F]]
}
