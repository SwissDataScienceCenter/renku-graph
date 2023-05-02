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

package io.renku.eventlog.events.producers
package awaitinggeneration

import AwaitingGenerationEventEncoder.{encodeEvent, encodePayload}
import cats.Parallel
import cats.effect._
import cats.syntax.all._
import eventdelivery._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.events.producers
import io.renku.eventlog.events.producers.DefaultSubscribers.DefaultSubscribers
import io.renku.eventlog.events.producers.DefaultSubscriberTracker
import io.renku.eventlog.metrics.{EventStatusGauges, QueriesExecutionTimes}
import io.renku.events.CategoryName
import io.renku.events.DefaultSubscription.DefaultSubscriber
import io.renku.graph.model.events.EventStatus
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

private[producers] object SubscriptionCategory {

  val categoryName: CategoryName = CategoryName("AWAITING_GENERATION")

  def apply[F[
      _
  ]: Async: Parallel: SessionResource: DefaultSubscriberTracker: Logger: MetricsRegistry: QueriesExecutionTimes: EventStatusGauges]
      : F[producers.SubscriptionCategory[F]] = for {
    implicit0(subscribers: DefaultSubscribers[F]) <- DefaultSubscribers[F](categoryName)
    eventFetcher                                  <- EventFinder[F]
    dispatchRecovery                              <- DispatchRecovery[F]
    eventDelivery <- eventdelivery.EventDelivery[F, AwaitingGenerationEvent](
                       eventDeliveryIdExtractor = (event: AwaitingGenerationEvent) => CompoundEventDeliveryId(event.id)
                     )
    eventsDistributor <- EventsDistributor(categoryName,
                                           subscribers,
                                           eventFetcher,
                                           eventDelivery,
                                           EventEncoder(encodeEvent, encodePayload),
                                           dispatchRecovery
                         )
  } yield new SubscriptionCategoryImpl[F, DefaultSubscriber](
    categoryName,
    subscribers,
    eventsDistributor,
    CapacityFinder.ofStatus(EventStatus.GeneratingTriples)
  )
}
