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
package tsmigrationrequest

import cats.effect.Async
import cats.syntax.all._
import eventdelivery.EventDelivery
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.events.producers
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

private[producers] object SubscriptionCategory {

  def apply[F[_]: Async: SessionResource: Logger: MetricsRegistry: QueriesExecutionTimes]
      : F[producers.SubscriptionCategory[F]] = for {
    implicit0(st: MigrationSubscriberTracker[F]) <- MigrationSubscriberTracker[F]
    subscribers                                  <- MigrationSubscribers[F](categoryName)
    eventFinder                                  <- EventFinder[F]
    dispatchRecovery                             <- DispatchRecovery[F]
    eventDelivery                                <- EventDelivery.noOp[F, MigrationRequestEvent]
    distributor <- EventsDistributor[F, MigrationRequestEvent](
                     categoryName,
                     subscribers,
                     eventFinder,
                     eventDelivery,
                     EventEncoder(MigrationRequestEvent.encodeEvent),
                     dispatchRecovery
                   )
  } yield new SubscriptionCategoryImpl(categoryName, subscribers, distributor, CapacityFinder.noOpCapacityFinder[F])
}
