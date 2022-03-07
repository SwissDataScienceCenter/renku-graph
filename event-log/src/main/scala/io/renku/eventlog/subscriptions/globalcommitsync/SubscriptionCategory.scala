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

package io.renku.eventlog.subscriptions.globalcommitsync

import cats.effect.Async
import cats.syntax.all._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.subscriptions
import io.renku.eventlog.subscriptions._
import io.renku.eventlog.subscriptions.eventdelivery._
import io.renku.eventlog.subscriptions.globalcommitsync.GlobalCommitSyncEventEncoder.encodeEvent
import io.renku.metrics.{LabeledHistogram, MetricsRegistry}
import org.typelevel.log4cats.Logger

private[subscriptions] object SubscriptionCategory {

  def apply[F[_]: Async: SessionResource: Logger: MetricsRegistry](
      queriesExecTimes:  LabeledHistogram[F, SqlStatement.Name],
      subscriberTracker: SubscriberTracker[F]
  ): F[subscriptions.SubscriptionCategory[F]] = for {
    subscribers           <- Subscribers(categoryName, subscriberTracker)
    lastSyncedDateUpdater <- LastSyncedDateUpdater(queriesExecTimes)
    eventsFinder          <- GlobalCommitSyncEventFinder(lastSyncedDateUpdater, queriesExecTimes)
    dispatchRecovery      <- DispatchRecovery(lastSyncedDateUpdater)
    eventDelivery         <- EventDelivery.noOp[F, GlobalCommitSyncEvent]
    eventsDistributor <- EventsDistributor(categoryName,
                                           subscribers,
                                           eventsFinder,
                                           eventDelivery,
                                           EventEncoder(encodeEvent),
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
