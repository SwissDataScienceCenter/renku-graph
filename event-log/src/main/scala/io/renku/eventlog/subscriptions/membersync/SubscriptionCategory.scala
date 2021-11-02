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

package io.renku.eventlog.subscriptions.membersync

import cats.effect.Async
import cats.syntax.all._
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.eventlog.subscriptions._
import io.renku.eventlog.subscriptions.membersync.MemberSyncEventEncoder.encodeEvent
import io.renku.eventlog.{EventLogDB, subscriptions}
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger

private[subscriptions] object SubscriptionCategory {

  def apply[F[_]: Async: Logger](sessionResource: SessionResource[F, EventLogDB],
                                 queriesExecTimes:  LabeledHistogram[F, SqlStatement.Name],
                                 subscriberTracker: SubscriberTracker[F]
  ): F[subscriptions.SubscriptionCategory[F]] = for {
    subscribers      <- Subscribers(categoryName, subscriberTracker)
    eventsFinder     <- MemberSyncEventFinder(sessionResource, queriesExecTimes)
    dispatchRecovery <- LoggingDispatchRecovery[F, MemberSyncEvent](categoryName)
    eventDelivery    <- EventDelivery.noOp[F, MemberSyncEvent]
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
