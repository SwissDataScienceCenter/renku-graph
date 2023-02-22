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
package globalcommitsync

import GlobalCommitSyncEventEncoder.encodeEvent
import cats.effect.Async
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.events.producers
import io.renku.eventlog.events.producers.DefaultSubscriberTracker
import io.renku.eventlog.events.producers.eventdelivery.EventDelivery
import io.renku.eventlog.events.producers.DefaultSubscribers.DefaultSubscribers
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.DefaultSubscription.DefaultSubscriber
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

private[producers] object SubscriptionCategory {

  def apply[F[_]: Async: SessionResource: DefaultSubscriberTracker: Logger: MetricsRegistry: QueriesExecutionTimes]
      : F[producers.SubscriptionCategory[F]] = for {
    implicit0(subscribers: DefaultSubscribers[F]) <- DefaultSubscribers[F](categoryName)
    lastSyncedDateUpdater                         <- LastSyncedDateUpdater[F]
    eventsFinder                                  <- EventFinder(lastSyncedDateUpdater)
    dispatchRecovery                              <- DispatchRecovery(lastSyncedDateUpdater)
    eventDelivery                                 <- EventDelivery.noOp[F, GlobalCommitSyncEvent]
    eventsDistributor <- EventsDistributor(categoryName,
                                           subscribers,
                                           eventsFinder,
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
