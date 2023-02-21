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
package cleanup

import CleanUpEventEncoder.encodeEvent
import cats.Parallel
import cats.effect._
import cats.syntax.all._
import eventdelivery._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.events.producers.awaitinggeneration.SubscriptionCategory.categoryName
import io.renku.eventlog.events.producers.DefaultSubscribers.DefaultSubscribers
import io.renku.eventlog.metrics.{EventStatusGauges, QueriesExecutionTimes}
import io.renku.events.DefaultSubscription.DefaultSubscriber
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

private[producers] object SubscriptionCategory {

  def apply[F[
      _
  ]: Async: Parallel: SessionResource: DefaultSubscriberTracker: Logger: MetricsRegistry: QueriesExecutionTimes: EventStatusGauges]
      : F[SubscriptionCategory[F]] = for {
    implicit0(subscribers: DefaultSubscribers[F]) <- DefaultSubscribers[F](categoryName)
    eventDelivery <- eventdelivery.EventDelivery[F, CleanUpEvent](
                       eventDeliveryIdExtractor = (event: CleanUpEvent) => DeletingProjectDeliverId(event.project.id)
                     )
    dispatchRecovery <- DispatchRecovery[F]
    eventFinder      <- EventFinder[F]
    eventsDistributor <- EventsDistributor(categoryName,
                                           subscribers,
                                           eventFinder,
                                           eventDelivery,
                                           EventEncoder(encodeEvent),
                                           dispatchRecovery
                         )
  } yield new SubscriptionCategoryImpl[F, DefaultSubscriber](categoryName,
                                                             subscribers,
                                                             eventsDistributor,
                                                             CapacityFinder.noOpCapacityFinder[F]
  )
}
